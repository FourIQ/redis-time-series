# frozen_string_literal: true

class Redis
  class TimeSeries
    # +Redis::TimeSeries::BatchCmd+ executes multiple RangeCmd objects in a single Redis pipeline, avoiding N round-trips when querying many series at once.

    # Commands that return false from +RangeCmd#batch_compatible?+ are executed serially via
    # +RangeCmd#cmd+ instead, allowing subclasses or extensions to opt out of pipelining
    # when their implementation already manages its own Redis connection or pipeline internally.

    # @example Batch-fetching samples for a collection of points
    #   range_cmds = points.map { |p| p.range_cmd(start_time:, end_time:, key: :original) }
    #   results = Redis::TimeSeries::BatchCmd.call(range_cmds)
    #   # results[i] is a Samples object corresponding to range_cmds[i]
    #
    class BatchCmd
      # Execute an array of RangeCmd objects as efficiently as possible.
      # Batch-compatible commands are pipelined together; incompatible ones run serially.
      #
      # @param range_cmds [Array<RangeCmd>] commands to execute.
      # @return [Array<Samples>] one Samples object per input RangeCmd, same order.
      def self.call(range_cmds)
        return [] if range_cmds.empty?

        results = Array.new(range_cmds.size)

        batch_indices, serial_indices = (0...range_cmds.size).partition { |i| range_cmds[i].batch_compatible? }

        unless batch_indices.empty?
          batch_cmds = batch_indices.map { |i| range_cmds[i] }
          raw_results = batch_cmds.first.timeseries.redis.with do |conn|
            conn.pipelined do |pipeline|
              batch_cmds.each { |rc| rc.timeseries.range_cmd(rc, pipeline: pipeline) }
            end
          end
          batch_indices.each_with_index do |original_idx, batch_pos|
            rows = raw_results[batch_pos]
            results[original_idx] = Samples.new(rows.filter_map { |ts, val| ts && Sample.new(ts, val) })
          end
        end

        serial_indices.each { |i| results[i] = range_cmds[i].cmd }

        results
      end
    end
  end
end
