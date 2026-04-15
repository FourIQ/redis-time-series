# frozen_string_literal: true

require "spec_helper"

RSpec.describe Redis::TimeSeries::BatchCmd do
  include RedisHelpers

  let(:ts1) { Redis::TimeSeries.new("batch_test_1", redis: redis) }
  let(:ts2) { Redis::TimeSeries.new("batch_test_2", redis: redis) }

  let(:t1) { Time.utc(2026, 1, 1, 0) }
  let(:t2) { Time.utc(2026, 1, 1, 1) }
  let(:t3) { Time.utc(2026, 1, 1, 2) }

  before do
    Redis::TimeSeries.redis = redis
    ts1.create
    ts2.create
    ts1.madd(t1 => 10, t2 => 20, t3 => 30)
    ts2.madd(t1 => 100, t2 => 200)
  end

  after { redis.with { |conn| conn.del(ts1.key, ts2.key) } }

  describe ".call" do
    context "with an empty array" do
      it { expect(described_class.call([])).to eq([]) }
    end

    context "with a single RangeCmd" do
      it "returns the expected samples" do
        rc = Redis::TimeSeries::RangeCmd.new(timeseries: ts1, start_time: t1, end_time: t3)
        expect(described_class.call([rc]).first.map(&:value)).to eq([BigDecimal("10"), BigDecimal("20"), BigDecimal("30")])
      end
    end

    context "with multiple RangeCmds" do
      let(:rc1) { Redis::TimeSeries::RangeCmd.new(timeseries: ts1, start_time: t1, end_time: t3) }
      let(:rc2) { Redis::TimeSeries::RangeCmd.new(timeseries: ts2, start_time: t1, end_time: t3) }

      it "returns one Samples object per command in order" do
        results = described_class.call([rc1, rc2])
        expect(results[0].map(&:value)).to eq([BigDecimal("10"), BigDecimal("20"), BigDecimal("30")])
        expect(results[1].map(&:value)).to eq([BigDecimal("100"), BigDecimal("200")])
      end

      it "executes in a single pipeline" do
        expect_any_instance_of(Redis).to receive(:pipelined).once.and_call_original
        described_class.call([rc1, rc2])
      end

      it "uses a single round-trip regardless of how many series are queried" do
        cmds = Array.new(10) { Redis::TimeSeries::RangeCmd.new(timeseries: ts1, start_time: t1, end_time: t3) }
        expect_any_instance_of(Redis).to receive(:pipelined).once.and_call_original
        expect(described_class.call(cmds).size).to eq(10)
      end

      it "returns empty Samples for a series with no data in range" do
        rc_empty = Redis::TimeSeries::RangeCmd.new(timeseries: ts2, start_time: t3 + 1.hour, end_time: t3 + 2.hours)
        results = described_class.call([rc1, rc_empty])
        expect(results[0]).not_to be_empty
        expect(results[1]).to be_empty
      end
    end
  end
end
