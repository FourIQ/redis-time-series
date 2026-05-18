# frozen_string_literal: true
require 'spec_helper'

RSpec.describe Redis::TimeSeries::RangeCmd do
  subject(:range) { described_class.new(timeseries: ts) }

  let(:key) { 'range_cmd_test' }
  let(:ts) { Redis::TimeSeries.create(key) }

  after { Redis::TimeSeries.redis.del(key) }

  describe '#options' do
    it 'returns the unbounded range by default' do
      expect(range.options).to eq ['-', '+']
    end

    it 'appends a COUNT option when set' do
      range.count = 5
      expect(range.options).to eq ['-', '+', ['COUNT', 5]]
    end

    it 'appends a FILTER_BY_TS option when set' do
      range.filter_by_ts = [100, 200]
      expect(range.options).to eq ['-', '+', ['FILTER_BY_TS', [100, 200]]]
    end

    it 'appends the aggregation when set' do
      range.aggregation = [:avg, 60_000]
      expect(range.options.last).to eq ['AGGREGATION', 'avg', 60_000]
    end

    it 'only emits ALIGN when an aggregation is configured' do
      range.align = 'start'
      expect(range.options.flatten).not_to include 'ALIGN'

      range.aggregation = [:avg, 60_000]
      expect(range.options.flatten).to include 'ALIGN'
    end
  end

  describe '#command' do
    it 'defaults to TS.RANGE' do
      expect(range.command).to eq 'TS.RANGE'
    end

    it 'switches to TS.REVRANGE when #revrange is called' do
      range.revrange
      expect(range.command).to eq 'TS.REVRANGE'
    end
  end

  describe '#cmd' do
    it 'returns the samples that match the range' do
      ts.madd(1_000 => 10, 2_000 => 20, 3_000 => 30)
      result = described_class.new(timeseries: ts, start_time: 1_000, end_time: 3_000).cmd
      expect(result.map(&:value)).to eq [BigDecimal('10'), BigDecimal('20'), BigDecimal('30')]
    end
  end
end
