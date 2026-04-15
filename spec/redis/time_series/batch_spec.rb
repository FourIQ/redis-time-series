# frozen_string_literal: true

require "spec_helper"

RSpec.describe Redis::TimeSeries::Batch do
  include RedisHelpers

  let(:key1) { "batch_test_1" }
  let(:key2) { "batch_test_2" }
  let(:ts1) { Redis::TimeSeries.new(key1, redis: redis) }
  let(:ts2) { Redis::TimeSeries.new(key2, redis: redis) }

  let(:t1) { Time.parse("2024-01-01 00:00:00") }
  let(:t2) { Time.parse("2024-01-01 01:00:00") }
  let(:t3) { Time.parse("2024-01-01 02:00:00") }

  before do
    Redis::TimeSeries.redis = redis
    ts1.create
    ts2.create
    ts1.madd(t1 => 10, t2 => 20, t3 => 30)
    ts2.madd(t1 => 100, t2 => 200)
  end

  after do
    redis.with { |conn| conn.del(key1, key2) }
  end

  describe ".call" do
    context "when given an empty array" do
      it "returns an empty array" do
        expect(described_class.call([])).to eq([])
      end
    end

    context "when given a single RangeCmd" do
      it "returns one Samples object" do
        rc = Redis::TimeSeries::RangeCmd.new(timeseries: ts1, start_time: t1, end_time: t3)
        results = described_class.call([rc])

        expect(results.length).to eq(1)
        expect(results.first).to be_a(Redis::TimeSeries::Samples)
        expect(results.first.map(&:value)).to eq([BigDecimal("10"), BigDecimal("20"), BigDecimal("30")])
      end
    end

    context "when given multiple RangeCmds" do
      it "returns one Samples object per command in the same order" do
        rc1 = Redis::TimeSeries::RangeCmd.new(timeseries: ts1, start_time: t1, end_time: t3)
        rc2 = Redis::TimeSeries::RangeCmd.new(timeseries: ts2, start_time: t1, end_time: t3)

        results = described_class.call([rc1, rc2])

        expect(results.length).to eq(2)
        expect(results[0].map(&:value)).to eq([BigDecimal("10"), BigDecimal("20"), BigDecimal("30")])
        expect(results[1].map(&:value)).to eq([BigDecimal("100"), BigDecimal("200")])
      end

      it "executes in a single pipeline" do
        rc1 = Redis::TimeSeries::RangeCmd.new(timeseries: ts1, start_time: t1, end_time: t3)
        rc2 = Redis::TimeSeries::RangeCmd.new(timeseries: ts2, start_time: t1, end_time: t3)

        pipeline_calls = 0
        allow(redis).to receive(:with).and_call_original
        conn_double = instance_double(Redis)
        allow(redis).to receive(:with).and_yield(conn_double)
        allow(conn_double).to receive(:pipelined) { |&block| pipeline_calls += 1 }

        described_class.call([rc1, rc2])

        expect(pipeline_calls).to eq(1)
      end

      it "returns empty Samples for a series with no data in range" do
        future_start = t3 + 1.hour
        rc1 = Redis::TimeSeries::RangeCmd.new(timeseries: ts1, start_time: t1, end_time: t3)
        rc_empty = Redis::TimeSeries::RangeCmd.new(timeseries: ts2, start_time: future_start, end_time: future_start + 1.hour)

        results = described_class.call([rc1, rc_empty])

        expect(results[0]).not_to be_empty
        expect(results[1]).to be_empty
      end
    end
  end
end
