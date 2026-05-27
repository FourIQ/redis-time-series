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
        it "returns one sample per sub-range, summed within the sub-range and aligned to the month bucket" do
          timestamp1 = Time.parse("2024-01-01")
          timestamp2 = Time.parse("2024-01-02")
          timestamp3 = Time.parse("2024-01-03")
          timestamp4 = Time.parse("2024-01-04")
          timestamp5 = Time.parse("2024-01-05")
          timestamp6 = Time.parse("2024-01-06")
          timestamp7 = Time.parse("2024-02-01")
          timestamp8 = Time.parse("2024-02-29")

          values = { timestamp1 => 10, timestamp2 => 30, timestamp3 => 40, timestamp4 => 45, timestamp5 => 100, timestamp6 => 50, timestamp7 => 50, timestamp8 => 50 }
          ts.madd(values)

          range_cmd = described_class.new(timeseries: ts, start_time: timestamp1, end_time: timestamp8.end_of_day)
          range_cmd.aggregation = ["sum", 2629746000]
          range_cmd.filter_by_range = [timestamp2..timestamp3, timestamp5..timestamp6]
          result = range_cmd.cmd
          # Two sub-ranges (both in Jan) → two TS.RANGE calls. Each returns one bucket aligned to Jan 1 with the sum inside that sub-range.
          # Feb has no covering sub-range → no Feb call, no Feb sample.
          expect(result.map { |sample| sample.time }).to eq([timestamp1, timestamp1])
          expect(result.map { |sample| sample.value.to_f }).to eq([70.0, 150.0])
        end
      end

      context "with filter_by_ts ≤128 timestamps" do
        it "emits one command per month (no slicing needed)" do
          timestamp1 = Time.parse("2024-01-01")
          timestamp_end = Time.parse("2024-04-01")

          range_cmd = described_class.new(timeseries: ts, start_time: timestamp1, end_time: timestamp_end)
          range_cmd.aggregation = ["avg", 2629746000]
          range_cmd.filter_by_ts = (0...100).map { |i| (timestamp1 + i.hours).to_i * 1000 }

          handle = nil
          Redis::TimeSeries.redis.with { |conn| conn.pipelined { |p| handle = range_cmd.enqueue(p) } }
          # 3 months × 1 chunk (100 ≤ 128) = 3 commands
          expect(handle.command_count).to eq(3)
          expect(handle.queried_timestamps.size).to eq(3)
        end
      end

      context "with filter_by_ts > 128 timestamps" do
        it "raises rather than silently producing per-chunk aggregates" do
          timestamp1 = Time.parse("2024-01-01")
          timestamp_end = Time.parse("2024-04-01")

          range_cmd = described_class.new(timeseries: ts, start_time: timestamp1, end_time: timestamp_end)
          range_cmd.aggregation = ["avg", 2629746000]
          range_cmd.filter_by_ts = (0...200).map { |i| (timestamp1 + i.hours).to_i * 1000 }

          expect { range_cmd.cmd }.to raise_error(ArgumentError, /FILTER_BY_TS combined with aggregation/)
        end
      end

      context "with both filter_by_ts and filter_by_range" do
        it "lets filter_by_ts win (consistent with daily and non-calendar)" do
          timestamp1 = Time.parse("2024-01-01")
          timestamp_end = Time.parse("2024-03-01")

          range_cmd = described_class.new(timeseries: ts, start_time: timestamp1, end_time: timestamp_end)
          range_cmd.aggregation = ["avg", 2629746000]
          range_cmd.filter_by_ts = [timestamp1.to_i * 1000]
          range_cmd.filter_by_range = [timestamp1..(timestamp1 + 1.day), (timestamp1 + 2.days)..(timestamp1 + 3.days)]

          handle = nil
          Redis::TimeSeries.redis.with { |conn| conn.pipelined { |p| handle = range_cmd.enqueue(p) } }
          # filter_by_ts wins → 1 command per month, 2 months → 2 commands (not 4 from per-sub-range slicing)
          expect(handle.command_count).to eq(2)
          # qts is now tracked per emitted command (not per iteration), so qts.size == command_count.
          expect(handle.queried_timestamps.size).to eq(2)
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
      it "returns one bucket per calendar day across the DST end transition" do
        timestamp1 = (winter_time - 2.days)
        timestamp2 = (winter_time - 1.day)
        timestamp3 = (winter_time)
        timestamp4 = (winter_time + 2.hours)
        timestamp5 = (winter_time + 3.hours)
        timestamp6 = (winter_time + 4.hours)
        timestamp7 = (winter_time + 1.days)
        timestamp8 = (winter_time + 2.days)

        values = { timestamp1 => 10, timestamp2 => 30, timestamp3 => 40, timestamp4 => 45, timestamp5 => 10, timestamp6 => 30, timestamp7 => 40, timestamp8 => 45 }
        ts.madd(values)

        range_cmd = described_class.new(timeseries: ts, start_time: timestamp1, end_time: timestamp8)
        range_cmd.aggregation = ["avg", 86400000]
        result = range_cmd.cmd
        # Daily aggregation produces one sample per calendar day.
        # Oct 27 (DST end day) contains four input timestamps t3..t6 — they collapse into ONE Oct 27 bucket: avg = (40+45+10+30)/4 = 31.25.
        # daily_aggregation restarts ALIGN at the DST boundary so post-DST buckets land on local midnight (+0100) instead of 23:00 from a UTC-rolled bucket.
        expect(result.map { |sample| sample.time }).to eq([timestamp1, timestamp2, timestamp3, timestamp7, timestamp8])
        expect(result.map { |sample| sample.value.to_f }).to eq([10.0, 30.0, 31.25, 40.0, 45.0])
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
