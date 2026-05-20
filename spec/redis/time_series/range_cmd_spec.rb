# frozen_string_literal: true

require "spec_helper"

RSpec.describe Redis::TimeSeries::RangeCmd do
  subject(:range) { described_class.new(timeseries: ts) }

  let(:key) { "time_series_test" }
  subject(:ts) { Redis::TimeSeries.create(key) }

  let(:summer_time) { Time.parse("2024-03-31") }
  let(:winter_time) { Time.parse("2024-10-27") }

  let(:key) { "range_test" }

  after { Redis::TimeSeries.redis.with{ |conn| conn.del(key) } }

  describe ".new" do
    it "returns an instance of RangeCmd" do
      expect(range).to be_a(described_class)
    end
  end

  describe "#options" do
    it "returns an array of options that are set" do
      expect(range.options).to be_an(Array)
    end
  end

  describe "#cmd" do
    it "calls cmd on the timeseries" do
      expect(range).to receive(:cmd)
      range.cmd
    end

    context "with an aggregation duration of 1.month" do
      it "returns an array of samples aggregated by the duration of that month" do
        timestamp1 = Time.parse("2024-01-01")
        timestamp2 = Time.parse("2024-02-01")
        timestamp3 = Time.parse("2024-03-01")
        timestamp4 = Time.parse("2024-04-01")

        values = { timestamp1 => 10, timestamp2 => 20, timestamp3 => 30 }
        ts.madd(values)

        range_cmd = described_class.new(timeseries: ts, start_time: timestamp1, end_time: timestamp4)
        range_cmd.aggregation = ["avg", 2629746000]
        result = range_cmd.cmd
        expect(result.map { |sample| sample.value }).to match_array([10, 20, 30])
        expect(result.map { |sample| sample.time }).to eq([timestamp1, timestamp2, timestamp3])
      end

      context "with filter_by_range" do
        it "returns monthly calculated values filtered by range" do
        timestamp1 = Time.parse("2024-01-01")
        timestamp2 = Time.parse("2024-01-02")
        timestamp3 = Time.parse("2024-01-03")
        timestamp4 = Time.parse("2024-01-04")
        timestamp5 = Time.parse("2024-01-05")
        timestamp6 = Time.parse("2024-01-06")
        timestamp7 = Time.parse("2024-02-01")
        timestamp8 = Time.parse("2024-02-29")

          values = { timestamp1 => 10, timestamp2 => 30, timestamp3 => 40, timestamp4 => 45, timestamp5 => 100, timestamp6 => 50, timestamp7 => 50, timestamp8 => 50}
          ts.madd(values)

          range_cmd = described_class.new(timeseries: ts, start_time: timestamp1, end_time: timestamp8.end_of_day)
          range_cmd.aggregation = ["sum", 2629746000]
          range_cmd.filter_by_range = [timestamp2..timestamp3,timestamp5..timestamp6]
          result = range_cmd.cmd#.filter_map { |sample| sample.value.nan? ? nil : sample }
          expect(result.map { |sample| sample.time }).to eq([timestamp1,timestamp8])
          expect(result.map { |sample| sample.value.to_f.round(1) }).to eq([35])
        end
      end

      context "with @empty" do
        it "returns a sample for missing months" do
          timestamp1 = Time.parse("2024-01-01")
          timestamp2 = Time.parse("2024-02-01")
          timestamp3 = Time.parse("2024-03-01")
          timestamp4 = Time.parse("2024-04-01")

          values = { timestamp1 => 10, timestamp3 => 20 }
          ts.madd(values)

          range_cmd = described_class.new(timeseries: ts, start_time: timestamp1, end_time: timestamp4)
          range_cmd.aggregation = ["avg", 2629746000]
          result = range_cmd.cmd
          expect(result.map { |sample| sample.time }).to eq([timestamp1, timestamp2, timestamp3])
        end
      end
    end

    context "with an aggregation duration of 1.day" do
      it "returns daily calculated values considering DST" do
        timestamp1 = (winter_time - 2.days)
        timestamp2 = (winter_time - 1.day)
        timestamp3 = (winter_time)
        timestamp4 = (winter_time + 2.hours)
        timestamp5 = (winter_time + 3.hours)
        timestamp6 = (winter_time + 4.hours)
        timestamp7 = (winter_time + 1.days)
        timestamp8 = (winter_time + 2.days)

        values = { timestamp1 => 10, timestamp2 => 30, timestamp3 => 40, timestamp4 => 45, timestamp5 => 10, timestamp6 => 30, timestamp7 => 40, timestamp8 => 45}
        ts.madd(values)

        range_cmd = described_class.new(timeseries: ts, start_time: timestamp1, end_time: timestamp8)
        range_cmd.aggregation = ["avg", 86400000]
        result = range_cmd.cmd.filter_map { |sample| sample.value.nan? ? nil : sample }
        expect(result.map { |sample| sample.time }).to eq([timestamp1, timestamp2, timestamp3, timestamp4, timestamp5, timestamp6, timestamp7,timestamp8])
        expect(result.map { |sample| sample.value.to_f.round(1) }).to eq([10, 30, 40, 45, 10, 30, 40, 45])
      end

      context "with filter_by_range" do
        it "returns daily calculated values filtered by range" do
        timestamp1 = Time.parse("2024-01-01")
        timestamp2 = Time.parse("2024-01-01") + 1.hour
        timestamp3 = Time.parse("2024-01-01") + 2.hours
        timestamp4 = Time.parse("2024-01-01") + 3.hours

          values = { timestamp1 => 10, timestamp2 => 30, timestamp3 => 40, timestamp4 => 45}
          ts.madd(values)

          range_cmd = described_class.new(timeseries: ts, start_time: timestamp1, end_time: timestamp4)
          range_cmd.aggregation = ["avg", 86400000]
          range_cmd.filter_by_range = [(timestamp2)..(timestamp3)]
          result = range_cmd.cmd#.filter_map { |sample| sample.value.nan? ? nil : sample }
          expect(result.map { |sample| sample.time }).to eq([timestamp1])
          expect(result.map { |sample| sample.value.to_f.round(1) }).to eq([35])
        end
      end
    end
  end

  describe "#revrange" do
    it "sets the command to TS.REVRANGE" do
      r = range
      r.revrange
      expect(r.command).to eq("TS.REVRANGE")
    end
  end

  describe "#enqueue" do
    it "queues commands on an external pipeline and returns a PipelineResult handle" do
      handle = nil
      Redis::TimeSeries.redis.with do |conn|
        conn.pipelined do |pipeline|
          handle = range.enqueue(pipeline)
        end
      end
      expect(handle).to be_a(Redis::TimeSeries::RangeCmd::PipelineResult)
      expect(handle.command_count).to eq(1)
      expect(handle).not_to be_a(Redis::TimeSeries::Samples)
    end

    it "tracks one command per month for a monthly aggregation" do
      timestamp1 = Time.parse("2024-01-01")
      timestamp4 = Time.parse("2024-04-01")
      range_cmd = described_class.new(timeseries: ts, start_time: timestamp1, end_time: timestamp4)
      range_cmd.aggregation = ["avg", 2629746000]

      handle = nil
      Redis::TimeSeries.redis.with do |conn|
        conn.pipelined do |pipeline|
          handle = range_cmd.enqueue(pipeline)
        end
      end
      expect(handle.command_count).to eq(3)
      expect(handle.queried_timestamps.size).to eq(3)
    end
  end
end

RSpec.describe Redis::TimeSeries::RangeCmd::Batch do
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

  it "returns Samples per RangeCmd in insertion order, equivalent to running each cmd individually" do
    timestamp1 = Time.parse("2024-01-01")
    timestamp2 = Time.parse("2024-01-02")
    timestamp3 = Time.parse("2024-01-03")

    ts1.madd({ timestamp1 => 10, timestamp2 => 20 })
    ts2.madd({ timestamp2 => 100, timestamp3 => 200 })

    expected1 = Redis::TimeSeries::RangeCmd.new(timeseries: ts1, start_time: timestamp1, end_time: timestamp3).cmd
    expected2 = Redis::TimeSeries::RangeCmd.new(timeseries: ts2, start_time: timestamp1, end_time: timestamp3).cmd

    batch_rc1 = Redis::TimeSeries::RangeCmd.new(timeseries: ts1, start_time: timestamp1, end_time: timestamp3)
    batch_rc2 = Redis::TimeSeries::RangeCmd.new(timeseries: ts2, start_time: timestamp1, end_time: timestamp3)
    batch_result = described_class.new.add(batch_rc1).add(batch_rc2).cmd

    expect(batch_result.size).to eq(2)
    expect(sample_pair(batch_result[0])).to eq(sample_pair(expected1))
    expect(sample_pair(batch_result[1])).to eq(sample_pair(expected2))
  end

  it "handles a mix of routing variants in a single pipeline" do
    timestamp1 = Time.parse("2024-01-01")
    timestamp2 = Time.parse("2024-02-01")
    timestamp3 = Time.parse("2024-03-01")
    timestamp4 = Time.parse("2024-04-01")

    ts1.madd({ timestamp1 => 10, timestamp2 => 20, timestamp3 => 30 })
    ts2.madd({ timestamp1 => 1, timestamp2 => 2, timestamp3 => 3 })

    monthly_rc = Redis::TimeSeries::RangeCmd.new(timeseries: ts1, start_time: timestamp1, end_time: timestamp4)
    monthly_rc.aggregation = ["avg", 2629746000]
    default_rc = Redis::TimeSeries::RangeCmd.new(timeseries: ts2, start_time: timestamp1, end_time: timestamp3)

    expected_monthly = Redis::TimeSeries::RangeCmd.new(timeseries: ts1, start_time: timestamp1, end_time: timestamp4).tap { |c| c.aggregation = ["avg", 2629746000] }.cmd
    expected_default = Redis::TimeSeries::RangeCmd.new(timeseries: ts2, start_time: timestamp1, end_time: timestamp3).cmd

    batch_result = described_class.new.add(monthly_rc).add(default_rc).cmd

    expect(sample_pair(batch_result[0])).to eq(sample_pair(expected_monthly))
    expect(sample_pair(batch_result[1])).to eq(sample_pair(expected_default))
  end

  it "returns an empty array when no RangeCmds are added" do
    expect(described_class.new.cmd).to eq([])
  end
end
