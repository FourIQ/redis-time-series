# frozen_string_literal: true

require "spec_helper"

RSpec.describe Redis::TimeSeries::RangeCmd do
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

  describe "#batch_compatible?" do
    it "returns true regardless of aggregation type" do
      [nil, 3600000, 86400000, 2629746000, 31556952000].each do |duration|
        rc = described_class.new(timeseries: ts1)
        rc.aggregation = [:avg, duration] if duration
        expect(rc).to be_batch_compatible
      end
    end
  end

  describe ".batch" do
    context "with an empty array" do
      it { expect(described_class.batch([])).to eq([]) }
    end

    context "with a single RangeCmd" do
      it "returns the expected samples" do
        rc = described_class.new(timeseries: ts1, start_time: t1, end_time: t3)
        expect(described_class.batch([rc]).first.map(&:value)).to eq([BigDecimal("10"), BigDecimal("20"), BigDecimal("30")])
      end
    end

    context "with multiple RangeCmds" do
      let(:rc1) { described_class.new(timeseries: ts1, start_time: t1, end_time: t3) }
      let(:rc2) { described_class.new(timeseries: ts2, start_time: t1, end_time: t3) }

      it "returns one Samples object per command in order" do
        results = described_class.batch([rc1, rc2])
        expect(results[0].map(&:value)).to eq([BigDecimal("10"), BigDecimal("20"), BigDecimal("30")])
        expect(results[1].map(&:value)).to eq([BigDecimal("100"), BigDecimal("200")])
      end

      it "executes in a single pipeline" do
        expect_any_instance_of(Redis).to receive(:pipelined).once.and_call_original
        described_class.batch([rc1, rc2])
      end

      it "uses a single round-trip regardless of how many series are queried" do
        cmds = Array.new(10) { described_class.new(timeseries: ts1, start_time: t1, end_time: t3) }
        expect_any_instance_of(Redis).to receive(:pipelined).once.and_call_original
        expect(described_class.batch(cmds).size).to eq(10)
      end

      it "returns empty Samples for a series with no data in range" do
        rc_empty = described_class.new(timeseries: ts2, start_time: t3 + 1.hour, end_time: t3 + 2.hours)
        results = described_class.batch([rc1, rc_empty])
        expect(results[0]).not_to be_empty
        expect(results[1]).to be_empty
      end
    end

    context "with a daily-aggregation command" do
      let(:rc_daily) do
        described_class.new(timeseries: ts1, start_time: t1, end_time: t3).tap do |rc|
          rc.aggregation = [:sum, 86400000]
        end
      end

      it "executes in the shared pipeline" do
        expect_any_instance_of(Redis).to receive(:pipelined).once.and_call_original
        described_class.batch([rc_daily])
      end

      it "returns a Samples result" do
        expect(described_class.batch([rc_daily]).first).to be_a(Redis::TimeSeries::Samples)
      end
    end

    context "with mixed regular and calendar-aggregation commands" do
      let(:rc_plain) { described_class.new(timeseries: ts1, start_time: t1, end_time: t3) }
      let(:rc_daily) do
        described_class.new(timeseries: ts2, start_time: t1, end_time: t3).tap do |rc|
          rc.aggregation = [:sum, 86400000]
        end
      end

      it "processes all commands in a single pipeline" do
        expect_any_instance_of(Redis).to receive(:pipelined).once.and_call_original
        described_class.batch([rc_plain, rc_daily])
      end

      it "returns results for all commands in order" do
        results = described_class.batch([rc_plain, rc_daily])
        expect(results[0].map(&:value)).to eq([BigDecimal("10"), BigDecimal("20"), BigDecimal("30")])
        expect(results[1]).to be_a(Redis::TimeSeries::Samples)
      end
    end
  end
end
