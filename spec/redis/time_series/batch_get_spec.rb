# frozen_string_literal: true

require "spec_helper"

RSpec.describe Redis::TimeSeries, ".batch_get" do
  let(:key1) { "batch_get_test_1" }
  let(:key2) { "batch_get_test_2" }
  let(:ts1) { Redis::TimeSeries.create(key1) }
  let(:ts2) { Redis::TimeSeries.create(key2) }

  after do
    Redis::TimeSeries.redis.with { |conn| conn.del(key1, key2) }
  end

  it "returns an empty array when no series are passed" do
    expect(described_class.batch_get([])).to eq([])
  end

  it "returns the most recent sample per series in input order, equivalent to calling #get individually" do
    timestamp1 = Time.parse("2024-01-01")
    timestamp2 = Time.parse("2024-01-02")

    ts1.madd({ timestamp1 => 10, timestamp2 => 20 })
    ts2.madd({ timestamp1 => 100 })

    expected1 = ts1.get
    expected2 = ts2.get

    result = described_class.batch_get([ts1, ts2])

    expect(result.size).to eq(2)
    expect([result[0].time, result[0].value]).to eq([expected1.time, expected1.value])
    expect([result[1].time, result[1].value]).to eq([expected2.time, expected2.value])
  end

  it "accepts string keys as well as TimeSeries objects" do
    timestamp = Time.parse("2024-01-01")
    ts1.madd({ timestamp => 10 })

    result = described_class.batch_get([key1])
    expect(result.first.value).to eq(10)
  end

  it "returns nil for empty series and for keys that do not exist" do
    timestamp = Time.parse("2024-01-01")
    ts1 # referencing the let creates the (empty) series
    ts2.madd({ timestamp => 5 })

    result = described_class.batch_get([ts1, "batch_get_test_does_not_exist", ts2])

    expect(result[0]).to be_nil
    expect(result[1]).to be_nil
    expect(result[2].value).to eq(5)
  end

  it "executes in a single pipeline round-trip regardless of how many series are queried" do
    timestamp = Time.parse("2024-01-01")
    ts1.madd({ timestamp => 10 })
    ts2.madd({ timestamp => 20 })

    series = [ts1, ts2, key1, key2, ts1]
    expect_any_instance_of(Redis).to receive(:pipelined).once.and_call_original
    expect(described_class.batch_get(series).size).to eq(5)
  end
end
