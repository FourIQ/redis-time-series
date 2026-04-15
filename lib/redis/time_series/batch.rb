# frozen_string_literal: true

class Redis
  class TimeSeries
    # +Redis::TimeSeries::Batch+ executes multiple RangeCmd objects in a single
    # Redis pipeline, avoiding N round-trips when querying many series at once.
    #
    # All RangeCmd objects must share the same Redis connection pool.
    # Only simple aggregations are supported (not monthly or yearly, which
    # perform their own internal per-period slicing inside RangeCmd#cmd).
    #
    # @example Batch-fetching samples for a collection of points
    #   range_cmds = points.map { |p| p.range_cmd(start_time:, end_time:, key: :original) }
    #   results = Redis::TimeSeries::Batch.call(range_cmds)
    #   # results[i] is a Samples object corresponding to range_cmds[i]
    #
    class Batch
      # Execute an array of RangeCmd objects in one pipelined Redis call.
      #
      # @param range_cmds [Array<RangeCmd>] commands to execute. Must be non-empty.
      # @return [Array<Samples>] one Samples object per input RangeCmd, same order.
      def self.call(range_cmds)
        return [] if range_cmds.empty?

        raw_results = range_cmds.first.timeseries.redis.with do |conn|
          conn.pipelined do |pipeline|
            range_cmds.each { |rc| rc.timeseries.range_cmd(rc, pipeline: pipeline) }
          end
        end

        raw_results.map do |rows|
          Samples.new(rows.filter_map { |ts, val| ts && Sample.new(ts, val) })
        end
      end
    end
  end
end
