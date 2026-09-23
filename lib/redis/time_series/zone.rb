# frozen_string_literal: true
class Redis
  class TimeSeries
    # A bare `Time.at` renders in the PROCESS zone (ENV["TZ"]), but the application reads a day, a
    # month or a sample's clock time in `Time.zone`. On a UTC-process host (the container default)
    # the two disagree silently. Same root cause as FourIQ/fouriq_shared_models#286.
    module Zone
      module_function

      # Rails' Time.zone when the host application has set one; nil for a standalone caller, which
      # keeps the process zone.
      def zone
        zone = Time.zone if Time.respond_to?(:zone)
        zone if zone.respond_to?(:at)
      end

      def at(seconds)
        (zone || Time).at(seconds)
      end

      # Rational, not `/ 1000`: integer division floors to the whole second (a run boundary is
      # `msec(grid) - 1`), and a Float lands nanoseconds short of the millisecond.
      def at_msec(msec)
        at(Rational(msec, 1000))
      end
    end
  end
end
