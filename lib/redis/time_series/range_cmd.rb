# frozen_string_literal: true

class Redis
  class TimeSeries
    # The +Redis::TimeSeries::RangeCmd+ class is used to chain options for the TS.RANGE command.
    #
    # A single command runs via #cmd. To execute many commands in one round trip, hand them to .batch — they are enqueued into a shared pipeline
    # and their replies are reassembled per command.
    class RangeCmd
      # Aggregation bucket durations (in ms) that need calendar slicing instead of fixed-duration buckets.
      DAILY_DURATION   = 86_400_000
      MONTHLY_DURATION = 2_629_746_000
      YEARLY_DURATION  = 31_556_952_000

      # Redis TimeSeries hard limit on FILTER_BY_TS list length.
      FILTER_BY_TS_LIMIT = 128

      attr_reader :command, :timeseries
      attr_accessor :filter_by_ts, :filter_by_range, :filter_by_value, :count, :align, :empty

      # ─── 1. Construction ──────────────────────────────────────────────

      def initialize(timeseries:, start_time: "-", end_time: "+")
        @timeseries = timeseries
        @start_time = start_time || "-"
        @end_time = end_time || "+"
        @command = "TS.RANGE"
        @align = "start"
        @empty = true
        @latest = false
        @aggregation = nil
      end

      # ─── 2. Configuration (chainable) ─────────────────────────────────

      # Calendar slicing reasons about wall-clock boundaries — beginning_of_year, beginning_of_day, a
      # DST transition — so these have to resolve in the zone the CALLER thinks in. See Zone.
      #
      # Rational, not `/ 1000`: integer division floors a run boundary (`msec(grid) - 1`) to the
      # whole second, losing the 999 ms between it and the next run; a Float is off by nanoseconds.
      def start_time
        Zone.at(@start_time.is_a?(Numeric) ? Rational(@start_time, 1000) : @start_time)
      end

      def end_time
        Zone.at(@end_time.is_a?(Numeric) ? Rational(@end_time, 1000) : @end_time)
      end

      def aggregation=(aggregation)
        @aggregation = Aggregation.parse(aggregation)
        self
      end

      def revrange
        @command = "TS.REVRANGE"
      end

      def options
        options = window_args
        # ALIGN only applies in combination with aggregation.
        options << ["ALIGN", @align] if @aggregation
        options << ["COUNT", @count] if @count
        options << @aggregation.to_a if @aggregation
        options << "empty" if @empty && @aggregation
        options << "latest" if @latest && @aggregation
        options
      end

      # ─── 3. Single-command execution ──────────────────────────────────

      # Runs this RangeCmd on its own and returns a Samples object.
      # exception: false so a missing key resolves to empty Samples (see
      # PipelineResult#sanitize) instead of raising.
      def cmd
        handle = nil
        pipeline_result = @timeseries.redis.with do |conn|
          conn.pipelined(exception: false) { |pipeline| handle = enqueue(pipeline) }
        end
        handle.resolve(pipeline_result)
      end

      # ─── 4. Batched execution ─────────────────────────────────────────
      #
      # Many RangeCmds in one round trip, in two phases:
      #
      #   a. Build phase — every RangeCmd writes its TS.RANGE calls into a shared pipeline; a CountingPipeline forwarder tracks how many slots it occupied and returns a PipelineResult handle.
      #   b. Read phase — once the pipeline flushes, each handle pulls its slice out of the shared result via an offset cursor.
      #
      # Slot accounting matters because a RangeCmd that fans out to many slots (calendar slicing or filter slicing) still needs its replies routed back together.
      #
      # Example with 3 cmds (A enqueues 1 slot, B enqueues 3, C enqueues 2):
      #   pipeline_result = [rA, rB1, rB2, rB3, rC1, rC2]
      #   walk:  A.consume(pipeline_result, 0) → [Samples_A, offset=1]
      #          B.consume(pipeline_result, 1) → [Samples_B, offset=4]
      #          C.consume(pipeline_result, 4) → [Samples_C, offset=6]
      #
      # Runs many RangeCmds in a single round trip. Returns an array of Samples objects in the same order as the input.
      def self.batch(range_cmds)
        return [] if range_cmds.empty?

        handles = []
        # exception: false keeps per-slot errors in place: one missing series
        # yields empty Samples for its own slot instead of failing every
        # RangeCmd in the batch (see PipelineResult#sanitize).
        pipeline_result = range_cmds.first.timeseries.redis.with do |conn|
          conn.pipelined(exception: false) do |pipeline|
            range_cmds.each { |range_cmd| handles << range_cmd.enqueue(pipeline) }
          end
        end

        offset = 0
        handles.map do |handle|
          samples, offset = handle.consume(pipeline_result, offset)
          samples
        end
      end

      # ─── 5. Enqueue + handle ──────────────────────────────────────────

      # Queue all underlying TS commands for this RangeCmd onto an externally-owned
      # pipeline and return a PipelineResult handle. The handle captures the
      # state needed to post-process this RangeCmd's slice of the shared
      # pipeline result into a Samples collection.
      def enqueue(pipeline)
        validate!
        counting_pipeline = CountingPipeline.new(pipeline)
        @slot_plan = []
        queried_timestamps = route_to_pipeline(counting_pipeline)
        PipelineResult.new(
          command_count: counting_pipeline.count,
          queried_timestamps: queried_timestamps,
          empty: @empty,
          slot_plan: @slot_plan
        )
      end

      # FILTER_BY_TS combined with aggregation can't be silently sliced: each
      # ≤128-timestamp chunk would yield its own per-bucket aggregate, not a
      # single aggregate over the full filter list. Below the limit is fine
      # (one chunk = one aggregate).
      def validate!
        return unless @aggregation && @filter_by_ts && @filter_by_ts.size > FILTER_BY_TS_LIMIT
        raise ArgumentError,
              "FILTER_BY_TS combined with aggregation cannot exceed #{FILTER_BY_TS_LIMIT} timestamps " \
              "(got #{@filter_by_ts.size}); slicing would yield per-chunk aggregates instead of one aggregate per bucket"
      end

      # Minimal forwarder that counts how many commands a RangeCmd queues onto
      # the underlying pipeline, so a PipelineResult knows which slice of the
      # shared pipeline result belongs to it.
      class CountingPipeline
        attr_reader :count

        def initialize(pipeline)
          @pipeline = pipeline
          @count = 0
        end

        def call(name, args)
          @count += 1
          @pipeline.call(name, args)
        end
      end
      private_constant :CountingPipeline

      # Handle returned from RangeCmd#enqueue. Resolves the slice of the shared
      # pipeline result array that belongs to a single RangeCmd into a Samples
      # collection, applying the same post-processing the inline #cmd does
      # (NaN injection for empty buckets).
      class PipelineResult
        attr_reader :command_count, :queried_timestamps

        def initialize(command_count:, queried_timestamps:, empty:, slot_plan:)
          @command_count = command_count
          @queried_timestamps = queried_timestamps || []
          @empty = empty
          @slot_plan = slot_plan
        end

        def empty?
          @empty
        end

        # Data commands only: the first/last-sample probes riding alongside are slots too (#22).
        def data_command_count
          @slot_plan.size
        end

        # Pull this handle's slice out of a shared pipeline result and return
        # [Samples, next_offset]. Used by RangeCmd.batch to walk a single
        # pipeline result across multiple RangeCmds.
        def consume(pipeline_result, offset = 0)
          slice = pipeline_result[offset, command_count] || []
          [resolve(slice), offset + command_count]
        end

        # Walks the pipeline slice that belongs to this RangeCmd and produces a Samples collection.
        #
        # When @empty was set and queried_timestamps is populated, the invariant queried_timestamps.size == slice.size holds — each pipeline reply corresponds to exactly one expected bucket. Empty replies are substituted with [bucket_ts, NaN].
        #
        # Otherwise (daily aggregation, non-calendar paths) the slice is flattened one level; rows already carry their own timestamps and no NaN injection is needed.
        def resolve(slice)
          slice = replies(slice.map { |raw| sanitize(raw) })
          rows =
            if @empty && !queried_timestamps.empty?
              slice.each_with_index.map do |raw, i|
                flat = raw.flatten(1)
                flat.empty? ? [queried_timestamps[i], BigDecimal("NaN")] : flat
              end
            else
              slice.flatten(1)
            end

          Samples.new(rows.filter_map { |timestamp, val| timestamp.nil? ? nil : Sample.new(timestamp, val) })
        end

        private
          # With pipelined(exception: false) a failed command surfaces as an
          # error object in its slot. A missing series only means "no data"
          # and becomes an empty reply; anything else is re-raised as
          # Redis::CommandError — the class plain pipelined raised before, so
          # callers' rescue contracts are unchanged.
          # One reply per data command, trimmed to the buckets holding its first and last sample;
          # a window with no sample at all keeps nothing, as 8.2 returned.
          def replies(slice)
            queue = slice.dup
            @slot_plan.map do |grid|
              reply = queue.shift || []
              next reply unless grid

              first, last = queue.shift.to_a.first, queue.shift.to_a.first
              next [] unless first && last

              low, high = bucket_of(first[0].to_i, grid), bucket_of(last[0].to_i, grid)
              reply.select { |row| row[0].to_i.between?(low, high) }
            end
          end

          def bucket_of(timestamp, grid)
            grid[:origin] + ((timestamp - grid[:origin]).div(grid[:duration]) * grid[:duration])
          end

          def sanitize(raw)
            return raw unless raw.is_a?(StandardError)
            raise Redis::CommandError, raw.message unless raw.message.include?(Redis::TimeSeries::MISSING_KEY_MESSAGE)
            []
          end
      end

      private
        # ─── 6. Routing ─────────────────────────────────────────────────

        def route_to_pipeline(pipeline)
          return enqueue_calendar_aggregation(pipeline) if calendar_aggregation?

          enqueue_window(pipeline)
          []
        end

        def calendar_aggregation?
          [DAILY_DURATION, MONTHLY_DURATION, YEARLY_DURATION].include?(@aggregation&.duration)
        end

        def enqueue_calendar_aggregation(pipeline)
          case @aggregation.duration
          when YEARLY_DURATION  then yearly_aggregation(pipeline)
          when MONTHLY_DURATION then monthly_aggregation(pipeline)
          when DAILY_DURATION   then daily_aggregation(pipeline)
          end
        end

        # Single window dispatch. Used both for the plain (no calendar) case and per-iteration by daily_aggregation, so each daily window still respects filter_by_ts / filter_by_range slicing.
        def enqueue_window(pipeline)
          if @filter_by_ts
            enqueue_filtered_by_ts(pipeline)
          elsif @filter_by_range
            enqueue_filtered_by_range(pipeline)
          else
            emit(pipeline)
          end
        end

        # ─── 7. Calendar slicing ────────────────────────────────────────
        #
        # Redis TimeSeries aggregations have a fixed bucket *duration*, not a calendar interval.
        # To aggregate per calendar year/month/day we issue one TS.RANGE per bucket and stitch the results back together in PipelineResult#resolve.

        def yearly_aggregation(pipeline)
          calendar_aggregation_loop(pipeline, initial_start: start_time.beginning_of_year, advance_by: :years, &:end_of_year)
        end

        def monthly_aggregation(pipeline)
          calendar_aggregation_loop(pipeline, initial_start: start_time, advance_by: :months, &:end_of_month)
        end

        # Generic per-calendar-period loop. The block returns this period's end-Time given the period's start; the caller supplies the very first start. Each iteration narrows @start_time/@end_time to the period, sets @aggregation to that period's exact duration, dispatches through enqueue_window, and pushes one queried_timestamps entry per command actually emitted (so qts.size == result.size when the post-processor needs to NaN-inject empty buckets).
        #
        # Requires `pipeline` to be a CountingPipeline — see enqueue.
        def calendar_aggregation_loop(pipeline, initial_start:, advance_by:)
          # `end_time`, not @end_time: Time#<=> hands a millisecond Integer on to Date#<=>, which
          # reads it as a Julian day number — a date the cursor never reaches, and the loop spins.
          window_end = end_time
          queried_timestamps = []

          preserving_state do
            current_start = initial_start
            current_end = (yield current_start) - 1
            while current_end < window_end
              self.aggregation = [@aggregation.type, ((current_end - current_start).round) * 1000]
              @start_time = current_start
              @end_time = current_end

              before = @slot_plan.size
              enqueue_window(pipeline)
              (@slot_plan.size - before).times { queried_timestamps << current_start.to_i * 1000 }

              current_start = current_start.advance(advance_by => 1)
              current_end = (yield current_start) - 1
            end
          end

          queried_timestamps
        end

        # One bucket per calendar day, counted from the window start. A Redis bucket is a fixed span of *elapsed* time, so the day a DST transition falls in is 23 or 25 hours long and every bucket after it drifts in wall-clock terms; that day is therefore asked for on its own, with its real length. Runs of ordinary 24-hour days still go out as one TS.RANGE each, so a year of daily buckets costs three commands, not 365.
        # Returns [] to match calendar_aggregation_loop's signature — daily replies carry per-bucket timestamps already, so no queried_timestamps tracking is needed.
        def daily_aggregation(pipeline)
          type = @aggregation.type

          preserving_state do
            day_runs.each do |run_start, run_end, bucket_duration|
              self.aggregation = [type, bucket_duration]
              @start_time = run_start
              @end_time = run_end
              enqueue_window(pipeline)
            end
          end

          []
        end

        # Splits the window into consecutive [start_ms, end_ms, bucket_duration_ms] runs: stretches of ordinary 24-hour days, and each transition day on its own with its real length.
        #
        # Deliberately not strided. Skipping ahead and comparing the offsets at the two ends of a stride cannot see a pair of transitions inside it whose offsets cancel, and tzdata carries 15 such pairs closer than a fortnight — the tightest America/Cambridge_Bay, 6.92 days in 2000. The stride is then skipped whole and NO transition is found, which is silently the behaviour this change exists to remove.
        def day_runs
          window_end = end_time
          runs = []
          run_start = grid = start_time

          while grid < window_end
            next_grid = next_day_boundary(grid)
            day_duration = ((next_grid - grid) * 1000).round

            if day_duration != DAILY_DURATION
              # A run ends one millisecond before the next one starts. Closing it a whole second
              # early would lose every sample in between; closing it exactly makes the two runs meet.
              runs << [msec(run_start), msec(grid) - 1, DAILY_DURATION] if grid > run_start
              runs << [msec(grid), [msec(next_grid) - 1, msec(window_end)].min, day_duration]
              run_start = next_grid
            end

            grid = next_grid
          end

          # `<=` covers a window ending exactly on a boundary — including a zero-length one
          # (from == to) — whose final instant would otherwise fall outside every run. `runs.empty?`
          # is for an INVERTED window (from > to), where the walk never runs and run_start is
          # already past the end: the loop this replaced always enqueued once, so it still does.
          runs << [msec(run_start), msec(window_end), DAILY_DURATION] if run_start <= window_end || runs.empty?
          runs
        end

        # With EMPTY, Redis 8.2 reported buckets from the first one holding data to the last; 8.10
        # fills empty ones out to the window's edges -- 0 for sum/count, the previous value for
        # `last` -- so they read as data. Each command therefore carries two probes, the first and
        # last sample of its own window under its own filters, and resolve trims to the buckets
        # between them. `twa` is left alone: it interpolates across bucket edges by design.
        def emit(pipeline)
          @timeseries.range_cmd(self, pipeline: pipeline)
          grid = bucket_grid
          @slot_plan << grid
          return unless grid

          # Through the series' own cmd, so the probes are encoded and debug-printed like the data command.
          probe = [*window_args, "COUNT", 1]
          @timeseries.send(:cmd, "TS.RANGE", @timeseries.key, probe, pipeline: pipeline)
          @timeseries.send(:cmd, "TS.REVRANGE", @timeseries.key, probe, pipeline: pipeline)
        end

        def bucket_grid
          return unless @aggregation && @empty && @aggregation.type.to_s != "twa"

          origin =
            case @align
            when "start", "-" then @start_time
            when "end", "+" then @end_time
            else @align
            end
          # An open bound ("-"/"+") is fine to probe, but not to build the grid on: Redis resolves it.
          return if origin.is_a?(String)

          { origin: msec(origin), duration: @aggregation.duration }
        end

        # The window and filters a command reads -- shared by the data command and its probes.
        def window_args
          args = [@start_time, @end_time]
          args << ["FILTER_BY_TS", @filter_by_ts] if @filter_by_ts
          args << ["FILTER_BY_VALUE", @filter_by_value] if @filter_by_value
          args
        end

        # Milliseconds from either form a bound arrives in, by Client.wire's rule, so the daily runs, the
        # bucket grid and a Time sent as-is all mean the same instant. Rounding put end_of_day on the next midnight.
        def msec(value)
          value.is_a?(Numeric) ? value.floor : Client.wire(value)
        end

        # The grid point a calendar day after `grid`, keeping its wall-clock time of day.
        #
        # Where the clock jumps AT midnight (Asia/Beirut, America/Santiago, America/Sao_Paulo) the
        # label moves onto the next date and STAYS there — that calendar day gets no bucket, and past
        # the transition the labels are no longer dates. No data is lost, every bucket is a true 24
        # hours, but a caller reading the timestamps as calendar days is reading them wrong.
        def next_day_boundary(grid)
          elapsed_day = grid + 86_400
          return elapsed_day if elapsed_day.utc_offset == grid.utc_offset

          adjusted = elapsed_day + (grid.utc_offset - elapsed_day.utc_offset)
          # A time of day the spring-forward skips does not exist on the transition day, so the
          # adjustment lands an hour before the gap instead of on it: that day is a plain 24h day.
          return elapsed_day unless adjusted.hour == grid.hour && adjusted.min == grid.min
          # An offset that moves by a whole day (Pacific/Apia 2011, Pacific/Kiritimati 1994) would
          # otherwise leave the grid standing still, and the caller hangs inside the pipeline block.
          return elapsed_day if adjusted <= grid

          adjusted
        end

        # ─── 8. Option slicing ──────────────────────────────────────────
        #
        # TS.RANGE only accepts up to 128 timestamps in FILTER_BY_TS, and FILTER_BY_RANGE is not a native Redis TimeSeries feature.
        # We implement both by emitting one TS.RANGE per slice/range and concatenating the replies on read.

        # Sub-ranges are CLIPPED to the window, not selected for sitting inside it. Requiring
        # containment dropped a range straddling the window's edge entirely, and daily aggregation
        # splits the window at every transition day — so a schedule covering the whole query, which
        # is what an UNCONFIGURED operational schedule produces, matched no run and returned empty
        # Samples for the entire read.
        def enqueue_filtered_by_range(pipeline)
          preserving_state do
            @align = @start_time
            clipped_ranges.each do |sub_range|
              @start_time = sub_range.begin
              @end_time = sub_range.end
              emit(pipeline)
            end
          end
        end

        # ⚠️ One command per sub-range, so a bucket holding two disjoint sub-ranges gets two rows at
        # the SAME timestamp, each aggregating its own part — `count`/`sum` still add up, two partial
        # `avg`s cannot be recombined. Redis cannot aggregate two disjoint intervals in one TS.RANGE
        # and the alternative is dropping one, which is the data loss this clipping exists to fix.
        # Pinned by a spec; only reachable off a midnight-aligned day grid, which no consumer uses.
        def clipped_ranges
          window_from = msec(start_time)
          window_to = msec(end_time)

          filter_by_range.filter_map do |sub_range|
            from = [msec(sub_range.begin), window_from].max
            to = [msec(sub_range.end), window_to].min
            (from..to) if from <= to
          end
        end

        def enqueue_filtered_by_ts(pipeline)
          preserving_state do
            @filter_by_ts.each_slice(FILTER_BY_TS_LIMIT) do |slice|
              @filter_by_ts = slice
              emit(pipeline)
            end
          end
        end

        # Every slicing path above narrows this RangeCmd's own state per emitted command. Restore it
        # on the way out, a raise inside the pipeline block included — otherwise the RangeCmd is left
        # pointing at one sub-window, with that sub-window's bucket size, and a caller that retries it
        # queries the wrong range.
        def preserving_state
          saved = [@start_time, @end_time, @aggregation, @align, @filter_by_ts]
          yield
        ensure
          @start_time, @end_time, @aggregation, @align, @filter_by_ts = saved
        end
    end
  end
end
