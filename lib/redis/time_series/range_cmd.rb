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

      def start_time
        Time.at(@start_time.is_a?(Numeric) ? @start_time / 1000 : @start_time)
      end

      def end_time
        Time.at(@end_time.is_a?(Numeric) ? @end_time / 1000 : @end_time)
      end

      def aggregation=(aggregation)
        @aggregation = Aggregation.parse(aggregation)
        self
      end

      def revrange
        @command = "TS.REVRANGE"
      end

      def options
        options = []
        options << @start_time
        options << @end_time
        options << ["FILTER_BY_TS", @filter_by_ts] if @filter_by_ts
        options << ["FILTER_BY_VALUE", @filter_by_value] if @filter_by_value
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
        queried_timestamps = route_to_pipeline(counting_pipeline)
        PipelineResult.new(
          command_count: counting_pipeline.count,
          queried_timestamps: queried_timestamps,
          empty: @empty,
          aggregation_duration: @aggregation&.duration
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
      # (NaN injection for empty buckets, DST artifact filtering, etc.).
      class PipelineResult
        attr_reader :command_count, :queried_timestamps, :aggregation_duration

        def initialize(command_count:, queried_timestamps:, empty:, aggregation_duration:)
          @command_count = command_count
          @queried_timestamps = queried_timestamps || []
          @empty = empty
          @aggregation_duration = aggregation_duration
        end

        def empty?
          @empty
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
          slice = slice.map { |raw| sanitize(raw) }
          rows =
            if @empty && !queried_timestamps.empty?
              slice.each_with_index.map do |raw, i|
                flat = raw.flatten(1)
                flat.empty? ? [queried_timestamps[i], BigDecimal("NaN")] : flat
              end
            else
              slice.flatten(1)
            end

          # Redis TimeSeries adds an extra row at the summer↔winter DST transition for daily aggregations. Drop it by keeping only rows whose HH:MM matches the first row's HH:MM.
          if aggregation_duration == DAILY_DURATION && !rows.empty?
            first_hhmm = Time.at(rows.first.first / 1000).strftime("%H:%M")
            rows = rows.select { |row| Time.at(row.first / 1000).strftime("%H:%M") == first_hhmm }
          end

          Samples.new(rows.filter_map { |timestamp, val| timestamp.nil? ? nil : Sample.new(timestamp, val) })
        end

        private
          # With pipelined(exception: false) a failed command surfaces as an
          # error object in its slot. A missing series only means "no data"
          # and becomes an empty reply; anything else is re-raised as
          # Redis::CommandError — the class plain pipelined raised before, so
          # callers' rescue contracts are unchanged.
          def sanitize(raw)
            return raw unless raw.is_a?(StandardError)
            raise Redis::CommandError, raw.message unless raw.message.include?(MISSING_KEY_MESSAGE)
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
            @timeseries.range_cmd(self, pipeline: pipeline)
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
          original_start_time = @start_time
          original_end_time = @end_time
          original_aggregation = @aggregation
          queried_timestamps = []

          current_start = initial_start
          current_end = (yield current_start) - 1
          while current_end < original_end_time
            self.aggregation = [@aggregation.type, ((current_end - current_start).round) * 1000]
            @start_time = current_start
            @end_time = current_end

            before = pipeline.count
            enqueue_window(pipeline)
            (pipeline.count - before).times { queried_timestamps << current_start.to_i * 1000 }

            current_start = current_start.advance(advance_by => 1)
            current_end = (yield current_start) - 1
          end

          @start_time = original_start_time
          @end_time = original_end_time
          @aggregation = original_aggregation
          queried_timestamps
        end

        # Walks day-by-day windows but breaks them at DST transitions, because Redis TimeSeries would otherwise drift by an hour for the rest of the year.
        # Returns [] to match calendar_aggregation_loop's signature — daily replies carry per-bucket timestamps already, so no queried_timestamps tracking is needed.
        def daily_aggregation(pipeline)
          current_start = start_time
          ts_end_time = end_time
          current_end = end_time - 1

          while current_end < ts_end_time
            tz = TZInfo::Timezone.get(Time.new(Time.now.year, 1, 1).zone)
            end_transition = tz.period_for_local(current_start).end_transition

            if end_transition
              day_after_dst_transition = Time.at(end_transition.timestamp_value + 1.day).beginning_of_day
              current_end = day_after_dst_transition < ts_end_time ? day_after_dst_transition - 1 : ts_end_time
              next_current_start = day_after_dst_transition
            else
              current_end = ts_end_time
              next_current_start = ts_end_time
            end

            @start_time = current_start
            @end_time = current_end
            enqueue_window(pipeline)
            current_start = next_current_start
          end

          []
        end

        # ─── 8. Option slicing ──────────────────────────────────────────
        #
        # TS.RANGE only accepts up to 128 timestamps in FILTER_BY_TS, and FILTER_BY_RANGE is not a native Redis TimeSeries feature.
        # We implement both by emitting one TS.RANGE per slice/range and concatenating the replies on read.

        def enqueue_filtered_by_range(pipeline)
          original_start_time = @start_time
          original_end_time = @end_time
          window = original_start_time..original_end_time
          sub_ranges = filter_by_range.select { |r| window.cover?(r) }

          @align = original_start_time
          sub_ranges.each do |sub_range|
            @start_time = sub_range.begin
            @end_time = sub_range.end
            @timeseries.range_cmd(self, pipeline: pipeline)
          end
          @start_time = original_start_time
          @end_time = original_end_time
        end

        def enqueue_filtered_by_ts(pipeline)
          original_filter_by_ts = @filter_by_ts
          original_filter_by_ts.each_slice(FILTER_BY_TS_LIMIT) do |slice|
            @filter_by_ts = slice
            @timeseries.range_cmd(self, pipeline: pipeline)
          end
          @filter_by_ts = original_filter_by_ts
        end
    end
  end
end
