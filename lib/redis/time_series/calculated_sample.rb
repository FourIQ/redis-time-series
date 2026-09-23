# this class takes all values instead of only BigDecimal like Redis::TimeSeries::Sample
# We can use this as an intermediate class, for instance to save an array of values in its @value variable
class Redis::TimeSeries::CalculatedSample < Redis::TimeSeries::Sample
  def initialize(timestamp, value)
    @ts_msec = timestamp
    @value = value
  end
end
