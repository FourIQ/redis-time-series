# frozen_string_literal: true

class Redis
  class TimeSeries
    class RangeCmd
      attr_reader :command, :timeseries
      attr_accessor :filter_by_ts, :filter_by_range, :filter_by_value, :count, :align, :empty

      # @param range_cmds [Array<RangeCmd>]
      # @return [Array<Samples>] one Samples object per input command, same order.
      def self.batch(range_cmds)
        return [] if range_cmds.empty?

        results = Array.new(range_cmds.size)
        queries = []

        raw_results = range_cmds.first.timeseries.redis.with do |conn|
          conn.pipelined do |pipeline|
            range_cmds.each_with_index { |rc, i| rc.enqueue_to_pipeline(pipeline).times { queries << i } }
          end
        end

        grouped = Hash.new { |h, k| h[k] = [] }
        raw_results.each_with_index { |row, j| grouped[queries[j]] << row }
        grouped.each { |original_idx, rows| results[original_idx] = range_cmds[original_idx].from_pipeline_results(rows) }

        results
      end

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
        options << ["ALIGN", @align] if @aggregation
        options << ["COUNT", @count] if @count
        options << @aggregation.to_a if @aggregation
        options << "empty" if @empty && @aggregation
        options << "latest" if @latest && @aggregation
        options
      end

      # Enqueues all TS.RANGE calls for this command into the given pipeline.Returns the number of pipeline slots added so RangeCmd.batch can map
      # each raw result back to its originating command.
      def enqueue_to_pipeline(pipeline)
        @queried_timestamps = []
        if calendar_aggregation?
          case @aggregation.duration
          when 31556952000
            @queried_timestamps = yearly_aggregation(pipeline)
            @queried_timestamps.size
          when 2629746000
            @queried_timestamps = monthly_aggregation(pipeline)
            @queried_timestamps.size
          when 86400000
            daily_aggregation(pipeline)
          end
        elsif @filter_by_ts
          sliced_cmd_for_filter_by_ts(pipeline)
        elsif @filter_by_range
          sliced_cmd_for_filter_by_range(pipeline)
        else
          @timeseries.range_cmd(self, pipeline: pipeline)
          1
        end
      end

      # Reassembles raw pipeline result rows into a Samples object.
      # rows: Array of raw TS.RANGE results for this command's pipeline slots.
      def from_pipeline_results(rows, queried_timestamps = nil)
        queried_timestamps = queried_timestamps || @queried_timestamps || []

        if @empty && queried_timestamps.any?
          rows.map! { |row| row.flatten!(1) }
          rows.map! do |row|
            timestamp = queried_timestamps.pop
            row.blank? ? [timestamp, BigDecimal("NaN")] : row
          end
        else
          rows.flatten!(1)
        end

        # Redis TimeSeries adds an extra record during DST transitions for daily aggregations.
        if @aggregation&.duration == 86400000 && rows.present?
          first_timestamp_time = Time.at(rows.first.first / 1000).strftime("%H:%M")
          rows = rows.select { |ts| Time.at(ts.first / 1000).strftime("%H:%M") == first_timestamp_time }
        end

        Samples.new(rows.filter_map { |timestamp, val| timestamp.nil? ? nil : Sample.new(timestamp, val) })
      end

      def cmd
        raw = @timeseries.redis.with do |conn|
          conn.pipelined do |pipeline|
            enqueue_to_pipeline(pipeline)
          end
        end
        from_pipeline_results(raw)
      end

      private

        def calendar_aggregation?
          [86400000, 2629746000, 31556952000].include?(@aggregation&.duration)
        end

        def yearly_aggregation(pipeline)
          original_start_time = @start_time
          original_end_time = @end_time
          original_aggregation = @aggregation
          queried_timestamps = []

          current_start = Time.at(start_time).beginning_of_year
          current_end = Time.at(start_time).end_of_year - 1
          while current_end < original_end_time
            self.aggregation = [@aggregation.type, ((current_end - current_start).round) * 1000]
            @start_time = current_start
            @end_time = current_end
            queried_timestamps << current_start.to_i * 1000

            if @filter_by_range
              sliced_cmd_for_filter_by_range(pipeline)
            else
              @timeseries.range_cmd(self, pipeline: pipeline)
            end

            current_start = Time.at(current_start).advance(years: 1)
            current_end = Time.at(current_start).end_of_year - 1
          end

          @start_time = original_start_time
          @end_time = original_end_time
          @aggregation = original_aggregation
          queried_timestamps.reverse!
        end

        def monthly_aggregation(pipeline)
          original_start_time = @start_time
          original_end_time = @end_time
          original_aggregation = @aggregation
          queried_timestamps = []

          current_start = Time.at(start_time)
          current_end = Time.at(start_time).end_of_month - 1
          while current_end < original_end_time
            self.aggregation = [@aggregation.type, ((current_end - current_start).round) * 1000]
            @start_time = current_start
            @end_time = current_end
            queried_timestamps << current_start.to_i * 1000

            if @filter_by_range
              sliced_cmd_for_filter_by_range(pipeline)
            else
              @timeseries.range_cmd(self, pipeline: pipeline)
            end

            current_start = Time.at(current_start).advance(months: 1)
            current_end = Time.at(current_start).end_of_month - 1
          end

          @start_time = original_start_time
          @end_time = original_end_time
          @aggregation = original_aggregation
          queried_timestamps.reverse!
        end

        def daily_aggregation(pipeline)
          count = 0
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

            count += if @filter_by_ts
              sliced_cmd_for_filter_by_ts(pipeline)
            elsif @filter_by_range
              sliced_cmd_for_filter_by_range(pipeline)
            else
              @timeseries.range_cmd(self, pipeline: pipeline)
              1
            end

            current_start = next_current_start
          end
          count
        end

        def sliced_cmd_for_filter_by_range(pipeline)
          start_time = @start_time
          end_time = @end_time
          ranges = filter_by_range.select { |f| (start_time..end_time).cover?(f) }
          @align = start_time
          ranges.each do |range|
            @start_time = range.begin
            @end_time = range.end
            @timeseries.range_cmd(self, pipeline: pipeline)
          end
          @start_time = start_time
          @end_time = end_time
          ranges.size
        end

        def sliced_cmd_for_filter_by_ts(pipeline)
          all_filter_by_ts = @filter_by_ts
          slices = all_filter_by_ts.each_slice(128).to_a
          slices.each do |filter_by_ts|
            @filter_by_ts = filter_by_ts
            @timeseries.range_cmd(self, pipeline: pipeline)
          end
          @filter_by_ts = all_filter_by_ts
          slices.size
        end
    end
  end
end
