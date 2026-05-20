# frozen_string_literal: true

class Redis
  class TimeSeries
    # The +Redis::TimeSeries::RangeCmd+ class is used to chain options for the TS.RANGE command
    class RangeCmd
      attr_reader :command, :timeseries
      attr_accessor :filter_by_ts, :filter_by_range, :filter_by_value, :count, :align, :empty

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
        # align can only be used with aggregation
        options << ["ALIGN", @align] if @aggregation
        options << ["COUNT", @count] if @count
        options << @aggregation.to_a if @aggregation
        options << "empty" if @empty && @aggregation
        options << "latest" if @latest && @aggregation
        options
      end

      # Queue all underlying TS commands for this RangeCmd onto an externally-owned
      # pipeline and return a PipelineResult handle. The handle captures the
      # state needed to post-process this RangeCmd's slice of the shared
      # pipeline result into a Samples collection.
      def enqueue(pipeline)
        counting_pipeline = CountingPipeline.new(pipeline)
        queried_timestamps = route_to_pipeline(counting_pipeline)
        PipelineResult.new(
          command_count: counting_pipeline.count,
          queried_timestamps: queried_timestamps,
          empty: @empty,
          aggregation_duration: @aggregation&.duration
        )
      end

      def cmd
        handle = nil
        pipeline_result = nil
        @timeseries.redis.with do |conn|
          pipeline_result = conn.pipelined do |pipeline|
            handle = enqueue(pipeline)
          end
        end
        handle.resolve(pipeline_result)
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
        # [Samples, next_offset]. Used by RangeCmd::Batch to walk a single
        # pipeline result across multiple RangeCmds.
        def consume(pipeline_result, offset = 0)
          slice = pipeline_result[offset, command_count] || []
          [resolve(slice), offset + command_count]
        end

        def resolve(slice)
          result = slice
          remaining_timestamps = queried_timestamps.dup

          # redis timeseries will return an empty array if there are no results.
          # if @empty is set we want a sample with NaN instead
          if @empty && !remaining_timestamps.empty?
            result.map! { |row| row.flatten!(1) }
            result.map! do |row|
              timestamp = remaining_timestamps.pop
              row.blank? ? [timestamp, BigDecimal("NaN")] : row
            end
          else
            result.flatten!(1)
          end

          # we need this because Redis Timeseries adds an extra record with a different time from the other records when transitioning from summer to winter time.
          if aggregation_duration == 86400000 && !result.blank?
            first_timestamp_time = Time.at(result.first.first / 1000).strftime("%H:%M")
            result = result.select do |ts|
              Time.at(ts.first / 1000).strftime("%H:%M") == first_timestamp_time
            end
          end

          Samples.new(result.filter_map { |timestamp, val| timestamp.nil? ? nil : Sample.new(timestamp, val) })
        end
      end

      private
        def route_to_pipeline(pipeline)
          case @aggregation&.duration
          when 31556952000
            return yearly_aggregation(pipeline)
          when 2629746000
            return monthly_aggregation(pipeline)
          when 86400000
            daily_aggregation(pipeline)
            return []
          end

          if @filter_by_ts
            sliced_cmd_for_filter_by_ts(pipeline)
          elsif @filter_by_range
            sliced_cmd_for_filter_by_range(pipeline)
          else
            @timeseries.range_cmd(self, pipeline: pipeline)
          end
          []
        end

        def yearly_aggregation(pipeline)
          original_start_time = @start_time
          original_end_time = @end_time
          original_aggregation = @aggregation
          queried_timestamps = []

          Redis::TimeSeries.new(@timeseries.key)
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

          Redis::TimeSeries.new(@timeseries.key)
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
          Redis::TimeSeries.new(@timeseries.key)

          # set up, make sure the while runs at least once
          current_start = Time.at(start_time)
          ts_end_time = Time.at(end_time)
          current_end = end_time - 1

          while current_end < ts_end_time
            tz = TZInfo::Timezone.get(Time.new(Time.now.year, 1, 1).zone)
            period = tz.period_for_local(current_start)
            end_transition = period.end_transition

            if end_transition
              # If there is a DST end transition
              day_after_dst_transition = Time.at(end_transition.timestamp_value + 1.day).beginning_of_day
              current_end = (day_after_dst_transition < ts_end_time ? Time.at(day_after_dst_transition) - 1 : ts_end_time)
              # Move start to just after the transition for next iteration
              next_current_start = day_after_dst_transition
            else
              # No DST: process the rest in one go and exit loop
              current_end = ts_end_time
              next_current_start = ts_end_time
            end

            @start_time = current_start
            @end_time = current_end

            if @filter_by_ts
              sliced_cmd_for_filter_by_ts(pipeline)
            elsif @filter_by_range
              sliced_cmd_for_filter_by_range(pipeline)
            else
              @timeseries.range_cmd(self, pipeline: pipeline)
            end

            current_start = next_current_start
          end
        end

        def sliced_cmd_for_filter_by_range(pipeline)
          result = []
          start_time = @start_time
          end_time = @end_time
          start_end_range = start_time..end_time
          @align = start_time
          filter_by_range.select { |f| start_end_range.cover?(f) }.each do |range|
            @start_time = range.begin
            @end_time = range.end
            result << @timeseries.range_cmd(self, pipeline: pipeline)
          end
          @start_time = start_time
          @end_time = end_time
          result
        end

        def sliced_cmd_for_filter_by_ts(pipeline)
          result = []
          all_filter_by_ts = @filter_by_ts
          all_filter_by_ts.each_slice(128) do |filter_by_ts|
            @filter_by_ts = filter_by_ts
            result << @timeseries.range_cmd(self, pipeline: pipeline)
          end
          @filter_by_ts = all_filter_by_ts
          result
        end
    end
  end
end
