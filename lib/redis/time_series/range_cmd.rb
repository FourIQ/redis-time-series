# frozen_string_literal: true

class Redis
  class TimeSeries
    # A single command runs via #cmd. To execute many commands in one round trip, hand them to .batch — they are enqueued into a shared pipeline
    # and their replies are reassembled per command.
    class RangeCmd
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
        @queried_timestamps = []
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
      def cmd
        raw_rows = @timeseries.redis.with do |conn|
          conn.pipelined { |pipeline| enqueue(pipeline) }
        end
        build_samples(raw_rows)
      end

      # ─── 4. Batched execution ─────────────────────────────────────────
      #
      # Many RangeCmds in one round trip, in two phases:
      #
      #   a. Build phase — every RangeCmd writes its TS.RANGE calls into a shared pipeline and reports how many pipeline slots it occupied.
      #   b. Read phase — once the pipeline flushes, every RangeCmd reassembles its own raw replies into a Samples object.
      #
      # Tracking slot ownership matters because a RangeCmd that fans out to many slots (calendar slicing or filter slicing) still needs its replies routed back together.
      # Runs many RangeCmds in a single round trip. Returns an array of Samples objects in the same order as the input.
      def self.batch(range_cmds)
        return [] if range_cmds.empty?

        slot_owner = []
        raw_rows = range_cmds.first.timeseries.redis.with do |conn|
          conn.pipelined do |pipeline|
            range_cmds.each_with_index do |range_cmd, owner_idx|
              range_cmd.enqueue(pipeline).times { slot_owner << owner_idx }
            end
          end
        end

        rows_per_owner = Hash.new { |h, k| h[k] = [] }
        raw_rows.each_with_index { |row, slot_idx| rows_per_owner[slot_owner[slot_idx]] << row }

        results = Array.new(range_cmds.size)
        rows_per_owner.each { |owner_idx, rows| results[owner_idx] = range_cmds[owner_idx].build_samples(rows) }
        results
      end

      # Writes every TS.RANGE call this RangeCmd needs into the given
      # pipeline. Returns the number of pipeline slots it occupied so the
      # caller (RangeCmd.batch) can route the raw replies back.
      def enqueue(pipeline)
        @queried_timestamps = []

        if calendar_aggregation?
          enqueue_calendar_aggregation(pipeline)
        elsif @filter_by_ts
          enqueue_filtered_by_ts(pipeline)
        elsif @filter_by_range
          enqueue_filtered_by_range(pipeline)
        else
          @timeseries.range_cmd(self, pipeline: pipeline)
          1
        end
      end

      # Turns the raw pipeline replies belonging to this RangeCmd into a
      # Samples object.
      def build_samples(raw_rows)
        rows = raw_rows

        # If @empty was set we want NaN samples back for buckets that Redis omitted.
        # Calendar slicing tracks the expected timestamp per slot in @queried_timestamps; we splice those in for the empty buckets.
        if @empty && @queried_timestamps.any?
          rows.map! { |row| row.flatten!(1) }
          rows.map! do |row|
            timestamp = @queried_timestamps.pop
            row.blank? ? [timestamp, BigDecimal("NaN")] : row
          end
        else
          rows.flatten!(1)
        end

        # Redis TimeSeries adds an extra row at the summer↔winter DST transition for daily aggregations. Drop it by keeping only rows whose HH:MM matches the first row's HH:MM.
        if @aggregation&.duration == 86400000 && rows.present?
          first_hhmm = Time.at(rows.first.first / 1000).strftime("%H:%M")
          rows = rows.select { |ts| Time.at(ts.first / 1000).strftime("%H:%M") == first_hhmm }
        end

        Samples.new(rows.filter_map { |timestamp, val| timestamp.nil? ? nil : Sample.new(timestamp, val) })
      end

      private
        # ─── 5. Dispatch helpers ────────────────────────────────────────

        def calendar_aggregation?
          [86400000, 2629746000, 31556952000].include?(@aggregation&.duration)
        end

        def enqueue_calendar_aggregation(pipeline)
          case @aggregation.duration
          when 31556952000 then yearly_aggregation(pipeline)
          when 2629746000  then monthly_aggregation(pipeline)
          when 86400000    then daily_aggregation(pipeline)
          end
        end

        # Used by daily_aggregation, which loops over multiple windows and still wants each window to respect filter_by_ts / filter_by_range slicing.
        def enqueue_window(pipeline)
          if @filter_by_ts
            enqueue_filtered_by_ts(pipeline)
          elsif @filter_by_range
            enqueue_filtered_by_range(pipeline)
          else
            @timeseries.range_cmd(self, pipeline: pipeline)
            1
          end
        end

        # ─── 6. Calendar slicing ────────────────────────────────────────
        #
        # Redis TimeSeries aggregations have a fixed bucket *duration*, not a calendar interval.
        # To aggregate per calendar year/month/day we issue one TS.RANGE per bucket and stitch the results back together in #build_samples.

        def yearly_aggregation(pipeline)
          original_start_time = @start_time
          original_end_time = @end_time
          original_aggregation = @aggregation
          slot_count = 0

          current_start = Time.at(start_time).beginning_of_year
          current_end = Time.at(start_time).end_of_year - 1
          while current_end < original_end_time
            self.aggregation = [@aggregation.type, ((current_end - current_start).round) * 1000]
            @start_time = current_start
            @end_time = current_end
            @queried_timestamps << current_start.to_i * 1000

            slot_count += if @filter_by_range
              enqueue_filtered_by_range(pipeline)
            else
              @timeseries.range_cmd(self, pipeline: pipeline)
              1
            end

            current_start = Time.at(current_start).advance(years: 1)
            current_end = Time.at(current_start).end_of_year - 1
          end

          @start_time = original_start_time
          @end_time = original_end_time
          @aggregation = original_aggregation
          @queried_timestamps.reverse!
          slot_count
        end

        def monthly_aggregation(pipeline)
          original_start_time = @start_time
          original_end_time = @end_time
          original_aggregation = @aggregation
          slot_count = 0

          current_start = Time.at(start_time)
          current_end = Time.at(start_time).end_of_month - 1
          while current_end < original_end_time
            self.aggregation = [@aggregation.type, ((current_end - current_start).round) * 1000]
            @start_time = current_start
            @end_time = current_end
            @queried_timestamps << current_start.to_i * 1000

            slot_count += if @filter_by_range
              enqueue_filtered_by_range(pipeline)
            else
              @timeseries.range_cmd(self, pipeline: pipeline)
              1
            end

            current_start = Time.at(current_start).advance(months: 1)
            current_end = Time.at(current_start).end_of_month - 1
          end

          @start_time = original_start_time
          @end_time = original_end_time
          @aggregation = original_aggregation
          @queried_timestamps.reverse!
          slot_count
        end

        # Walks day-by-day windows but breaks them at DST transitions, because Redis TimeSeries would otherwise drift by an hour for the rest of the year.
        def daily_aggregation(pipeline)
          slot_count = 0
          current_start = Time.at(start_time)
          ts_end_time = Time.at(end_time)
          current_end = end_time - 1

          while current_end < ts_end_time
            tz = TZInfo::Timezone.get(Time.new(Time.now.year, 1, 1).zone)
            period = tz.period_for_local(current_start)
            end_transition = period.end_transition

            if end_transition
              day_after_dst_transition = Time.at(end_transition.timestamp_value + 1.day).beginning_of_day
              current_end = (day_after_dst_transition < ts_end_time ? Time.at(day_after_dst_transition) - 1 : ts_end_time)
              next_current_start = day_after_dst_transition
            else
              current_end = ts_end_time
              next_current_start = ts_end_time
            end

            @start_time = current_start
            @end_time = current_end
            slot_count += enqueue_window(pipeline)
            current_start = next_current_start
          end
          slot_count
        end

        # ─── 7. Option slicing ──────────────────────────────────────────
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
          sub_ranges.size
        end

        def enqueue_filtered_by_ts(pipeline)
          original_filter_by_ts = @filter_by_ts
          slices = original_filter_by_ts.each_slice(128).to_a
          slices.each do |slice|
            @filter_by_ts = slice
            @timeseries.range_cmd(self, pipeline: pipeline)
          end
          @filter_by_ts = original_filter_by_ts
          slices.size
        end
    end
  end
end
