# frozen_string_literal: true
require 'spec_helper'

RSpec.describe Redis::TimeSeries::Samples do
  let(:sample_class) { Redis::TimeSeries::Sample }
  let(:samples) do
    described_class.new([
      sample_class.new(1_000, '1'),
      sample_class.new(2_000, '2'),
      sample_class.new(3_000, '3')
    ])
  end

  describe '#to_a' do
    it 'returns [time, value] tuples' do
      expect(samples.to_a).to eq([
        [Time.at(1), BigDecimal('1')],
        [Time.at(2), BigDecimal('2')],
        [Time.at(3), BigDecimal('3')]
      ])
    end

    it 'can emit raw millisecond timestamps' do
      expect(samples.to_a(raw_timestamps: true).map(&:first)).to eq [1_000, 2_000, 3_000]
    end
  end

  describe '#to_h' do
    it 'returns a time => value hash' do
      expect(samples.to_h).to eq(
        Time.at(1) => BigDecimal('1'),
        Time.at(2) => BigDecimal('2'),
        Time.at(3) => BigDecimal('3')
      )
    end
  end

  describe '.merge' do
    let(:other) do
      described_class.new([
        sample_class.new(1_000, '10'),
        sample_class.new(2_000, '20')
      ])
    end

    it 'with :keep_all keeps every timestamp from any set' do
      merged = described_class.merge(sample_sets: [samples, other])
      expect(merged.map(&:to_msec)).to eq [1_000, 2_000, 3_000]
      expect(merged.map(&:value)).to eq [[BigDecimal('1'), BigDecimal('10')],
                                         [BigDecimal('2'), BigDecimal('20')],
                                         [BigDecimal('3')]]
    end

    it 'with :keep_equal keeps only timestamps in every set' do
      merged = described_class.merge(sample_sets: [samples, other], merge_strategy: :keep_equal)
      expect(merged.map(&:to_msec)).to eq [1_000, 2_000]
    end

    it 'with :keep_first keeps only timestamps present in the first set' do
      merged = described_class.merge(sample_sets: [samples, other], merge_strategy: :keep_first)
      expect(merged.map(&:to_msec)).to eq [1_000, 2_000, 3_000]
    end
  end

  describe '#sum_values!' do
    it 'reduces each enumerable value to its sum' do
      merged = described_class.merge(sample_sets: [samples, samples])
      merged.sum_values!
      expect(merged.map(&:value)).to eq [BigDecimal('2'), BigDecimal('4'), BigDecimal('6')]
    end

    it 'raises CalculationError on non-enumerable values' do
      expect { samples.sum_values! }.to raise_error(Redis::TimeSeries::CalculationError)
    end
  end

  describe '#avg_values!' do
    it 'reduces each value list to its mean, ignoring NaN' do
      merged = described_class.merge(sample_sets: [samples, samples])
      merged.avg_values!
      expect(merged.map(&:value)).to eq [BigDecimal('1'), BigDecimal('2'), BigDecimal('3')]
    end
  end

  describe '#filter_nan!' do
    let(:nan_samples) do
      described_class.new([
        sample_class.new(1_000, '1'),
        sample_class.new(2_000, BigDecimal('NaN'))
      ])
    end

    it 'replaces NaN with the provided default' do
      nan_samples.filter_nan!(new_value: -1)
      expect(nan_samples.map(&:value)).to eq [BigDecimal('1'), -1]
    end
  end
end
