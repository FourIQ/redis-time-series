# frozen_string_literal: true
require 'delegate'

class Redis
  class TimeSeries
    # +Redis::TimeSeries::Samples+ is a thin wrapper around an array of
    # {Sample} objects with convenience helpers for post-processing query
    # results: converting to plain arrays/hashes, merging multiple result
    # sets by timestamp, and reducing the merged values with a chosen
    # aggregation (sum/avg/min/max/...).
    #
    # The reducer methods mutate the underlying Sample values, which is why
    # {Sample#value} is writable.
    class Samples < DelegateClass(Array)
      attr_accessor :metadata

      def to_a(raw_timestamps: false)
        map { |sample| [raw_timestamps ? sample.to_msec : sample.time, sample.value] }
      end

      def to_h(raw_timestamps: false)
        to_a(raw_timestamps: raw_timestamps).to_h
      end

      # Merge several Samples sets into one, keyed by timestamp. The resulting
      # samples have a CalculatedSample with an array of values, which can
      # then be reduced via the +*_values!+ methods.
      #
      # @param sample_sets [Array<Samples>]
      # @param merge_strategy [Symbol]
      #   - +:keep_all+ (default) keeps every timestamp seen in any set
      #   - +:keep_equal+ keeps only timestamps present in every set
      #   - +:keep_first+ keeps only timestamps present in the first set
      def self.merge(sample_sets:, merge_strategy: :keep_all)
        samples_hash = {}
        sample_sets.each_with_index do |samples, index|
          samples.each do |sample|
            sample_default =
              if merge_strategy != :keep_first || index.zero?
                CalculatedSample.new(sample.to_msec, [])
              end
            calculated_sample = samples_hash.fetch(sample.time, sample_default)
            next if calculated_sample.nil?
            calculated_sample.value << sample.value
            samples_hash[sample.time] = calculated_sample
          end
        end
        samples = new(samples_hash.values)
        samples.select! { |sample| sample.value.count == sample_sets.count } if merge_strategy == :keep_equal
        samples.metadata = sample_sets.filter_map(&:metadata).inject({}) { |result, m| m.merge(result) }
        samples
      end

      def sum_values!
        reduce_enumerable_values! { |values| nan_zeroed(values).sum }
      end

      def subtract_values!
        reduce_enumerable_values! do |values|
          values.reduce(values.first * 2) { |result, v| result - v }
        end
      end

      def avg_values!
        reduce_enumerable_values! do |values|
          cleaned = reject_nan(values)
          cleaned.empty? ? Float::NAN : cleaned.sum / cleaned.length
        end
      end

      def min_values!
        reduce_enumerable_values! do |values|
          cleaned = reject_nan(values)
          cleaned.empty? ? Float::NAN : cleaned.min
        end
      end

      def max_values!
        reduce_enumerable_values! do |values|
          cleaned = reject_nan(values)
          cleaned.empty? ? Float::NAN : cleaned.max
        end
      end

      def multiply_values!(factor:)
        each { |sample| sample.value *= factor }
        self
      end

      def divide_values!(factor:)
        each { |sample| sample.value /= factor }
        self
      end

      def round_values!(...)
        each { |sample| sample.value = sample.value.round(...) }
        self
      end

      def filter_nan!(new_value: 0)
        each { |sample| sample.value = new_value if sample.value.respond_to?(:nan?) && sample.value.nan? }
        self
      end

      private

      def reduce_enumerable_values!
        each do |sample|
          unless sample.value.is_a?(Enumerable)
            raise CalculationError, "expected an enumerable in sample.value, but sample is #{sample.inspect}"
          end
          sample.value = yield(sample.value)
        end
        self
      end

      def reject_nan(values)
        values.reject { |v| v.respond_to?(:nan?) && v.nan? }
      end

      def nan_zeroed(values)
        values.map { |v| v.respond_to?(:nan?) && v.nan? ? 0 : v }
      end
    end
  end
end
