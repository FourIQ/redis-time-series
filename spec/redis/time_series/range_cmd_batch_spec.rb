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

  it "matches single execution for filter_by_range (schedule-shaped), alone and mixed with a plain cmd" do
    day = Time.parse("2024-01-01")
    ts1.madd((0...48).to_h { |i| [day + i * 30 * 60, i] })
    ts2.madd({ day => 1, day + 1.hour => 2 })

    open_ranges = [(day + 9.hours)..(day + 12.hours), (day + 13.hours)..(day + 17.hours)]
    build_filtered = lambda do
      rc = described_class.new(timeseries: ts1, start_time: day, end_time: day + 1.day)
      rc.filter_by_range = open_ranges
      rc
    end
    build_plain = -> { described_class.new(timeseries: ts2, start_time: day, end_time: day + 1.day) }

    expected_filtered = build_filtered.call.cmd
    expected_plain = build_plain.call.cmd

    expect_any_instance_of(Redis).to receive(:pipelined).once.and_call_original
    batch_result = described_class.batch([build_filtered.call, build_plain.call])

    expect(sample_pair(batch_result[0])).to eq(sample_pair(expected_filtered))
    expect(batch_result[0]).not_to be_empty
    expect(sample_pair(batch_result[1])).to eq(sample_pair(expected_plain))
  end

  it "matches single execution for filter_by_value, plain and combined with aggregation" do
    base = Time.parse("2024-01-01")
    ts1.madd({ base => 1, base + 1.minute => 5, base + 2.minutes => 9, base + 3.minutes => 4 })

    build_plain = lambda do
      rc = described_class.new(timeseries: ts1, start_time: base, end_time: base + 1.hour)
      rc.filter_by_value = [3, 8]
      rc
    end
    build_agg = lambda do
      rc = described_class.new(timeseries: ts1, start_time: base, end_time: base + 1.hour)
      rc.filter_by_value = [3, 8]
      rc.aggregation = ["count", 3_600_000]
      rc
    end

    expected_plain = build_plain.call.cmd
    expected_agg = build_agg.call.cmd

    expect_any_instance_of(Redis).to receive(:pipelined).once.and_call_original
    batch_result = described_class.batch([build_plain.call, build_agg.call])

    expect(sample_pair(batch_result[0])).to eq(sample_pair(expected_plain))
    expect(batch_result[0]).not_to be_empty
    expect(sample_pair(batch_result[1])).to eq(sample_pair(expected_agg))
    expect(batch_result[1]).not_to be_empty
  end

  # The comfort-monitoring shape: heaviest fan-out in production (daily calendar slicing × schedule sub-ranges).
  it "matches single execution for daily calendar aggregation + filter_by_range + filter_by_value in a batch" do
    start_time = Time.parse("2024-01-01")
    end_time = Time.parse("2024-01-04")
    ts1.madd((0...72).to_h { |h| [start_time + h.hours, h % 24] })
    ts2.madd({ start_time => 1, start_time + 1.day => 2 })

    open_ranges = (0..2).map { |d| (start_time + d.days + 9.hours)..(start_time + d.days + 17.hours) }
    build_comfort = lambda do
      rc = described_class.new(timeseries: ts1, start_time: start_time, end_time: end_time)
      rc.aggregation = ["count", 86_400_000]
      rc.filter_by_range = open_ranges
      rc.filter_by_value = [10, 16]
      rc
    end
    build_other = -> { described_class.new(timeseries: ts2, start_time: start_time, end_time: end_time) }

    expected_comfort = build_comfort.call.cmd
    expected_other = build_other.call.cmd

    expect_any_instance_of(Redis).to receive(:pipelined).once.and_call_original
    batch_result = described_class.batch([build_comfort.call, build_other.call])

    expect(sample_pair(batch_result[0])).to eq(sample_pair(expected_comfort))
    expect(batch_result[0]).not_to be_empty
    expect(sample_pair(batch_result[1])).to eq(sample_pair(expected_other))
  end

  it "matches single execution and TS.GET for the revrange + count 1 last-sample shape" do
    base = Time.parse("2024-01-01")
    ts1.madd({ base => 1, base + 1.minute => 2, base + 2.minutes => 3 })

    build = lambda do
      rc = described_class.new(timeseries: ts1)
      rc.revrange
      rc.count = 1
      rc
    end

    expected = build.call.cmd
    last = ts1.get

    expect_any_instance_of(Redis).to receive(:pipelined).once.and_call_original
    batch_result = described_class.batch([build.call])

    expect(sample_pair(batch_result[0])).to eq(sample_pair(expected))
    expect(batch_result[0].first.time).to eq(last.time)
    expect(batch_result[0].first.value).to eq(last.value)
  end

  it "slices filter_by_ts beyond 128 timestamps inside a batch without disturbing neighbors" do
    base = Time.parse("2024-01-01")
    times = (0...150).map { |i| base + i.minutes }
    ts1.madd(times.each_with_index.to_h { |t, i| [t, i] })
    ts2.madd({ base => 1 })

    filter_ts = times.map { |t| t.to_i * 1000 }
    build_filtered = lambda do
      rc = described_class.new(timeseries: ts1, start_time: base, end_time: base + 1.day)
      rc.filter_by_ts = filter_ts
      rc
    end
    build_plain = -> { described_class.new(timeseries: ts2, start_time: base, end_time: base + 1.day) }

    expected_filtered = build_filtered.call.cmd
    expected_plain = build_plain.call.cmd

    expect_any_instance_of(Redis).to receive(:pipelined).once.and_call_original
    batch_result = described_class.batch([build_filtered.call, build_plain.call])

    expect(sample_pair(batch_result[0])).to eq(sample_pair(expected_filtered))
    expect(batch_result[0].size).to eq(150)
    expect(sample_pair(batch_result[1])).to eq(sample_pair(expected_plain))
  end

  it "keeps the offset cursor aligned across fan-out, plain, fan-out ordering" do
    jan = Time.parse("2024-01-01")
    apr = Time.parse("2024-04-01")
    ts1.madd({ jan => 10, Time.parse("2024-02-01") => 20, Time.parse("2024-03-01") => 30 })
    ts2.madd({ jan => 1, jan + 1.hour => 2 })

    build_monthly = lambda do
      rc = described_class.new(timeseries: ts1, start_time: jan, end_time: apr)
      rc.aggregation = ["avg", 2_629_746_000]
      rc
    end
    build_plain = -> { described_class.new(timeseries: ts2, start_time: jan, end_time: apr) }
    build_ranged = lambda do
      rc = described_class.new(timeseries: ts1, start_time: jan, end_time: apr)
      rc.filter_by_range = [jan..(jan + 1.day), Time.parse("2024-02-01")..Time.parse("2024-02-02")]
      rc
    end

    expected = [build_monthly.call.cmd, build_plain.call.cmd, build_ranged.call.cmd]

    expect_any_instance_of(Redis).to receive(:pipelined).once.and_call_original
    batch_result = described_class.batch([build_monthly.call, build_plain.call, build_ranged.call])

    batch_result.each_with_index do |samples, i|
      expect(sample_pair(samples)).to eq(sample_pair(expected[i]))
      expect(samples).not_to be_empty
    end
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
