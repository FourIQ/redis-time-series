# frozen_string_literal: true

require "spec_helper"

# Calendar buckets have to follow the APPLICATION's zone (Rails' Time.zone), not the process zone
# that ENV["TZ"] sets. A bare `Time.at` renders in the latter, which on a UTC-process host — the
# container default — aligned every day/month/year bucket to UTC boundaries and left
# daily_aggregation's DST break unreachable, since the zone it asked about had none.
#
# Every example here sets a process zone DIFFERENT from Time.zone, which is the only arrangement
# that can tell the two apart. The existing range_cmd_spec covers the other half — no Time.zone set,
# process zone honoured — so both paths stay pinned.
RSpec.describe Redis::TimeSeries::RangeCmd, "calendar buckets and time zones" do
  let(:key) { "range_zone_test" }
  subject(:ts) { Redis::TimeSeries.create(key) }

  around do |example|
    original_tz = ENV.fetch("TZ", nil)
    original_zone = Time.zone
    ENV["TZ"] = process_zone
    Time.zone = app_zone
    example.run
  ensure
    ENV["TZ"] = original_tz
    Time.zone = original_zone
  end

  after { Redis::TimeSeries.redis.with { |conn| conn.del(key) } }

  context "with the process on UTC and the application on Amsterdam" do
    let(:process_zone) { "UTC" }
    let(:app_zone) { "Europe/Amsterdam" }

    # The same case range_cmd_spec pins under an Amsterdam process zone: one bucket per calendar
    # day across the DST end, with Oct 27's four samples collapsing into one bucket. The answer must
    # not depend on which zone the host happens to be in.
    it "breaks daily buckets at the application zone's DST transition" do
      midnight = Time.zone.parse("2024-10-27")
      timestamps = [midnight - 2.days, midnight - 1.day, midnight,
                    midnight + 2.hours, midnight + 3.hours, midnight + 4.hours,
                    midnight + 1.day, midnight + 2.days]
      values = [10, 30, 40, 45, 10, 30, 40, 45]
      ts.madd(timestamps.zip(values).to_h)

      range_cmd = described_class.new(timeseries: ts, start_time: timestamps.first, end_time: timestamps.last)
      range_cmd.aggregation = ["avg", 86_400_000]
      result = range_cmd.cmd

      expect(result.map { |sample| sample.time.to_i })
        .to eq([timestamps[0], timestamps[1], timestamps[2], timestamps[6], timestamps[7]].map(&:to_i))
      expect(result.map { |sample| sample.value.to_f }).to eq([10.0, 30.0, 31.25, 40.0, 45.0])
    end

    # Amsterdam's Jan 1 is 23:00 UTC on Dec 31, so a yearly bucket aligned in the process zone lands
    # an hour into the previous year — and takes an hour of December's samples with it.
    it "aligns a yearly bucket on the application zone's new year" do
      new_year = Time.zone.parse("2024-01-01")
      ts.madd({ new_year + 1.hour => 10, new_year + 1.day => 30 })

      range_cmd = described_class.new(timeseries: ts, start_time: new_year, end_time: Time.zone.parse("2025-01-02"))
      range_cmd.aggregation = ["avg", 31_556_952_000]

      expect(range_cmd.cmd.map { |sample| sample.time.to_i }.first).to eq(new_year.to_i)
    end

    # `end_of_month` decides where February's window stops. Resolved in the process zone that is an
    # hour into March local time, so the first hour of March was averaged into February's bucket —
    # the bucket timestamp looks right and only the value is wrong, which is the kind of error that
    # survives a glance at a graph.
    it "ends a monthly window at the application zone's month boundary" do
      february = Time.zone.parse("2024-02-01")
      march = Time.zone.parse("2024-03-01")
      ts.madd({ february + 1.hour => 10, march + 30.minutes => 100 })

      range_cmd = described_class.new(timeseries: ts, start_time: february, end_time: Time.zone.parse("2024-04-01"))
      range_cmd.aggregation = ["avg", 2_629_746_000]
      buckets = range_cmd.cmd.map { |sample| [sample.ts_msec, sample.value.to_f] }

      expect(buckets.first).to eq([february.to_i * 1000, 10.0])
    end

    it "reports start_time and end_time in the application zone" do
      range_cmd = described_class.new(timeseries: ts, start_time: Time.zone.parse("2024-06-01").to_i * 1000,
                                                     end_time: Time.zone.parse("2024-06-02").to_i * 1000)

      expect(range_cmd.start_time.zone).to eq("CEST")
      expect(range_cmd.end_time.utc_offset).to eq(7200)
    end
  end

  # No application zone at all: the gem is usable outside Rails, so the process zone stays the
  # fallback rather than becoming UTC by default.
  context "with no application zone set" do
    let(:process_zone) { "Europe/Amsterdam" }
    let(:app_zone) { nil }

    it "falls back to the process zone" do
      range_cmd = described_class.new(timeseries: ts, start_time: Time.parse("2024-06-01").to_i * 1000,
                                                     end_time: Time.parse("2024-06-02").to_i * 1000)

      expect(range_cmd.start_time.zone).to eq("CEST")
    end
  end

  # A zone whose abbreviation is a numeric offset is not a TZInfo identifier, so asking TZInfo for
  # it raised from inside a range query. An offset-only zone genuinely has no transitions, which is
  # what the daily loop now assumes for it.
  context "with a process zone that abbreviates to a numeric offset and no application zone" do
    let(:process_zone) { "Pacific/Kiritimati" }
    let(:app_zone) { nil }

    it "queries without raising instead of dying on TZInfo::InvalidTimezoneIdentifier" do
      start_time = Time.parse("2024-06-01")
      ts.madd({ start_time + 1.hour => 10, start_time + 2.hours => 30 })

      range_cmd = described_class.new(timeseries: ts, start_time: start_time, end_time: start_time + 3.hours)
      range_cmd.aggregation = ["avg", 86_400_000]

      expect { range_cmd.cmd }.not_to raise_error
    end
  end
end
