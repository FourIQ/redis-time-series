# frozen_string_literal: true
class Redis
  class TimeSeries
    # A sample is an immutable value object that represents a single data point within a time series.
    class Sample

      # @return [Integer] the sample's timestamp in milliseconds
      attr_reader :ts_msec
      # @return [BigDecimal] the decimal value of the sample
      attr_accessor :value

      # Samples are returned by time series query methods, there's no need to create one yourself.
      # @api private
      # @see TimeSeries#get
      # @see TimeSeries#range
      def initialize(timestamp, value)
        @ts_msec = timestamp
        # RESP3 sends a value as a Float, which BigDecimal before 4.0 refuses without a precision; its
        # shortest string is exactly the text RESP2 sends.
        @value = BigDecimal(value.is_a?(Float) ? value.to_s : value)
      end

      # Built on first read: most callers only ever touch the value or ts_msec.
      # @return [Time, ActiveSupport::TimeWithZone] the sample's timestamp, to the millisecond, in
      #   the application's Time.zone when one is set, otherwise in the process zone
      def time
        @time ||= Zone.at_msec(ts_msec)
      end

      # @return [Hash] a hash representation of the sample
      # @example
      #   {:timestamp=>1595199272401, :value=>0.2e1}
      def to_h
        {
          timestamp: ts_msec,
          value: value
        }
      end
    end
  end
end
