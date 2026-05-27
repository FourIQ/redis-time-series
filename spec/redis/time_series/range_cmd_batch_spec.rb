# frozen_string_literal: true

require "spec_helper"

RSpec.describe Redis::TimeSeries::RangeCmd, ".batch" do
  let(:key1) { "batch_range_test_1" }
  let(:key2) { "batch_range_test_2" }
  let(:ts1) { Redis::TimeSeries.create(key1) }
  let(:ts2) { Redis::TimeSeries.create(key2) }

  after do
    Redis::TimeSeries.redis.with { |conn| conn.del(key1, key2) }
  end

  def sample_pair(samples)
    samples.map { |s| [s.time, s.value.respond_to?(:nan?) && s.value.nan? ? :nan : s.value.to_f] }
  end

  it "returns an empty array when no RangeCmds are passed" do
    expect(described_class.batch([])).to eq([])
  end

  it "returns Samples per RangeCmd in input order, equivalent to running each cmd individually" do
    timestamp1 = Time.parse("2024-01-01")
    timestamp2 = Time.parse("2024-01-02")
    timestamp3 = Time.parse("2024-01-03")

    ts1.madd({ timestamp1 => 10, timestamp2 => 20 })
    ts2.madd({ timestamp2 => 100, timestamp3 => 200 })

    expected1 = described_class.new(timeseries: ts1, start_time: timestamp1, end_time: timestamp3).cmd
    expected2 = described_class.new(timeseries: ts2, start_time: timestamp1, end_time: timestamp3).cmd

    batch_rc1 = described_class.new(timeseries: ts1, start_time: timestamp1, end_time: timestamp3)
    batch_rc2 = described_class.new(timeseries: ts2, start_time: timestamp1, end_time: timestamp3)
    batch_result = described_class.batch([batch_rc1, batch_rc2])

    expect(batch_result.size).to eq(2)
    expect(sample_pair(batch_result[0])).to eq(sample_pair(expected1))
    expect(sample_pair(batch_result[1])).to eq(sample_pair(expected2))
  end

  it "executes in a single pipeline round-trip" do
    timestamp1 = Time.parse("2024-01-01")
    timestamp2 = Time.parse("2024-01-02")
    ts1.madd({ timestamp1 => 10, timestamp2 => 20 })
    ts2.madd({ timestamp1 => 100, timestamp2 => 200 })

    rc1 = described_class.new(timeseries: ts1, start_time: timestamp1, end_time: timestamp2)
    rc2 = described_class.new(timeseries: ts2, start_time: timestamp1, end_time: timestamp2)

    expect_any_instance_of(Redis).to receive(:pipelined).once.and_call_original
    described_class.batch([rc1, rc2])
  end

  it "uses a single round-trip regardless of how many series are queried" do
    timestamp1 = Time.parse("2024-01-01")
    timestamp2 = Time.parse("2024-01-02")
    ts1.madd({ timestamp1 => 10, timestamp2 => 20 })

    cmds = Array.new(10) { described_class.new(timeseries: ts1, start_time: timestamp1, end_time: timestamp2) }
    expect_any_instance_of(Redis).to receive(:pipelined).once.and_call_original
    expect(described_class.batch(cmds).size).to eq(10)
  end

  it "handles a mix of routing variants in a single pipeline" do
    timestamp1 = Time.parse("2024-01-01")
    timestamp2 = Time.parse("2024-02-01")
    timestamp3 = Time.parse("2024-03-01")
    timestamp4 = Time.parse("2024-04-01")

    ts1.madd({ timestamp1 => 10, timestamp2 => 20, timestamp3 => 30 })
    ts2.madd({ timestamp1 => 1, timestamp2 => 2, timestamp3 => 3 })

    monthly_rc = described_class.new(timeseries: ts1, start_time: timestamp1, end_time: timestamp4)
    monthly_rc.aggregation = ["avg", 2629746000]
    default_rc = described_class.new(timeseries: ts2, start_time: timestamp1, end_time: timestamp3)

    expected_monthly = described_class.new(timeseries: ts1, start_time: timestamp1, end_time: timestamp4).tap { |c| c.aggregation = ["avg", 2629746000] }.cmd
    expected_default = described_class.new(timeseries: ts2, start_time: timestamp1, end_time: timestamp3).cmd

    expect_any_instance_of(Redis).to receive(:pipelined).once.and_call_original
    batch_result = described_class.batch([monthly_rc, default_rc])

    expect(sample_pair(batch_result[0])).to eq(sample_pair(expected_monthly))
    expect(sample_pair(batch_result[1])).to eq(sample_pair(expected_default))
  end
end
