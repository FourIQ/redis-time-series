# frozen_string_literal: true
class Redis
  class TimeSeries
    # +CalculatedSample+ is a Sample variant whose +value+ may hold any type
    # (typically an Array of values gathered by {Samples.merge}) rather than
    # being coerced to BigDecimal. It is used as an intermediate before the
    # +Samples#*_values!+ reducers collapse the values back into scalars.
    class CalculatedSample < Sample
      def initialize(timestamp, value)
        @time = Time.at(timestamp / 1000.0)
        @value = value
      end
    end
  end
end
