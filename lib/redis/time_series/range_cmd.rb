# frozen_string_literal: true
require 'active_support/core_ext/date'
require 'active_support/core_ext/numeric/time'
require 'active_support/core_ext/time'
require 'tzinfo'

class Redis
  class TimeSeries
    # +Redis::TimeSeries::RangeCmd+ is a chainable builder around the
    # +TS.RANGE+ / +TS.REVRANGE+ commands.
    #
    # Set query options (count, aggregation, filters) on a RangeCmd and call
    # {#cmd} to execute. When aggregating by a calendar interval (day, month,
    # year) RangeCmd issues one underlying query per calendar bucket and
    # concatenates the results, so callers see results aligned to real
    # calendar boundaries instead of fixed-length millisecond buckets. Daily
    # aggregations are also DST-aware.
    #
    # @example
    #   range = Redis::TimeSeries::RangeCmd.new(
    #     timeseries: ts,
    #     start_time: 1.month.ago,
    #     end_time: Time.current
    #   )
    #   range.aggregation = [:avg, 1.day]
    #   range.cmd
    class RangeCmd
      DAILY_MS = 86_400_000
      MONTHLY_MS = 2_629_746_000
      YEARLY_MS = 31_556_952_000

      attr_reader :command, :timeseries
      attr_accessor :filter_by_ts, :filter_by_range, :filter_by_value,
                    :count, :align, :empty

      def initialize(timeseries:, start_time: '-', end_time: '+')
        @timeseries = timeseries
        @start_time = start_time || '-'
        @end_time = end_time || '+'
        @command = 'TS.RANGE'
        @aggregation = nil
      end

      def aggregation=(aggregation)
        @aggregation = Aggregation.parse(aggregation)
      end

      def revrange
        @command = 'TS.REVRANGE'
        self
      end

      # @return [Array] the trailing options for the underlying TS.RANGE
      #   command (everything after the key).
      def options
        options = []
        options << @start_time
        options << @end_time
        options << ['FILTER_BY_TS', @filter_by_ts] if @filter_by_ts
        options << ['FILTER_BY_VALUE', @filter_by_value] if @filter_by_value
        options << ['ALIGN', @align] if @align && @aggregation
        options << ['COUNT', @count] if @count
        options << @aggregation.to_a if @aggregation
        options << 'EMPTY' if @empty && @aggregation
        options
      end

      # Execute the range query and return the resulting samples.
      #
      # @return [Array<Sample>]
      def cmd
        rows = collect_rows

        if @aggregation&.duration == DAILY_MS && !rows.empty?
          # RedisTimeSeries returns an extra row at the DST summer->winter
          # transition because two local times collide. Trim it.
          first_hm = Time.at(rows.first.first / 1000).strftime('%H:%M')
          rows.select! do |timestamp, _|
            Time.at(timestamp / 1000).strftime('%H:%M') == first_hm
          end
        end

        rows.map { |timestamp, value| Sample.new(timestamp, value) }
      end

      private

      def collect_rows
        case @aggregation&.duration
        when YEARLY_MS  then collect_yearly
        when MONTHLY_MS then collect_monthly
        when DAILY_MS   then collect_daily
        else                 single_call
        end
      end

      def single_call
        @timeseries.send(:range_cmd, self)
      end

      def collect_yearly
        slice_aggregation { |slice_start| slice_start.advance(years: 1).beginning_of_year - 1 }
      end

      def collect_monthly
        slice_aggregation { |slice_start| slice_start.end_of_month - 1 }
      end

      def slice_aggregation
        original_start = @start_time
        original_end = @end_time
        original_aggregation = @aggregation
        rows = []
        queried_timestamps = []

        current_start = to_time(original_start)
        current_end = yield(current_start)

        while current_end < to_time(original_end)
          self.aggregation = [@aggregation.type, ((current_end - current_start).round) * 1000]
          @start_time = current_start
          @end_time = current_end

          result = @timeseries.send(:range_cmd, self)
          if @empty && result.empty?
            rows << [current_start.to_i * 1000, BigDecimal('NaN')]
          else
            rows.concat(result)
          end
          queried_timestamps << current_start.to_i * 1000

          advance_by = @aggregation.duration == YEARLY_MS ? { years: 1 } : { months: 1 }
          current_start = current_start.advance(**advance_by)
          current_end = yield(current_start)
        end

        @start_time = original_start
        @end_time = original_end
        @aggregation = original_aggregation
        rows
      end

      def collect_daily
        original_start = @start_time
        original_end = @end_time
        rows = []

        current_start = to_time(original_start)
        ts_end_time = to_time(original_end)
        current_end = ts_end_time - 1
        tz = TZInfo::Timezone.get(Time.new(Time.now.year, 1, 1).zone)

        while current_end < ts_end_time
          period = tz.period_for_local(current_start)
          end_transition = period.end_transition

          if end_transition
            day_after_dst = (Time.at(end_transition.timestamp_value).utc.localtime + 86_400).beginning_of_day
            current_end = day_after_dst < ts_end_time ? day_after_dst - 1 : ts_end_time
            next_current_start = day_after_dst
          else
            current_end = ts_end_time
            next_current_start = ts_end_time
          end

          @start_time = current_start
          @end_time = current_end
          rows.concat(@timeseries.send(:range_cmd, self))
          current_start = next_current_start
        end

        @start_time = original_start
        @end_time = original_end
        rows
      end

      def to_time(value)
        return value if value.is_a?(Time)
        return Time.at(value.to_i / 1000) if value.is_a?(Numeric)
        value
      end
    end
  end
end
