# frozen_string_literal: true

class Redis
  class TimeSeries
    class RangeCmd
      # Drive multiple RangeCmds through a single Redis pipeline. Each added
      # RangeCmd is enqueued onto one shared pipeline; after the pipeline
      # returns, each RangeCmd's slice of the result is resolved into a Samples
      # collection. Result order matches insertion order.
      #
      # All RangeCmds passed in are assumed to share the Redis connection used
      # by this batch (defaults to Redis::TimeSeries.redis).
      class Batch
        attr_reader :range_cmds

        def initialize(redis: Redis::TimeSeries.redis)
          @redis = redis
          @range_cmds = []
        end

        def add(range_cmd)
          @range_cmds << range_cmd
          self
        end
        alias_method :<<, :add

        def cmd
          return [] if @range_cmds.empty?

          handles = []
          pipeline_result = nil
          @redis.with do |conn|
            pipeline_result = conn.pipelined do |pipeline|
              @range_cmds.each { |range_cmd| handles << range_cmd.enqueue(pipeline) }
            end
          end

          offset = 0
          handles.map do |handle|
            samples, offset = handle.consume(pipeline_result, offset)
            samples
          end
        end
      end
    end
  end
end
