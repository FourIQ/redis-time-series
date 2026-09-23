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
    def counting_pipeline
      Class.new do
        attr_reader :count, :calls

        def initialize
          @count = 0
          @calls = []
        end

        def call(name, args)
          @count += 1
          @calls << [name, args]
        end

        # The first/last-sample probes each aggregated EMPTY command carries (#22).
        def probes
          @calls.count { |_name, args| args.last(2) == %w[COUNT 1] && !args.include?("AGGREGATION") }
        end

        def data_commands
          count - probes
        end
      end.new
    end

    # Fails part-way through a multi-command enqueue, the way a dropped connection would.
    def exploding_pipeline
      Class.new do
        def initialize
          @calls = 0
        end

        def call(_name, _args)
          @calls += 1
          raise "boom" if @calls == 2
        end
      end.new
    end

    it "calls cmd on the timeseries" do
      expect(range).to receive(:cmd)
      range.cmd
    end

    context "when the series key does not exist" do
      let(:missing_ts) { Redis::TimeSeries.new("range_test_missing_key") }

      it "returns empty Samples instead of raising" do
        cmd = described_class.new(timeseries: missing_ts, start_time: Time.parse("2024-01-01"), end_time: Time.parse("2024-01-02"))

        expect(cmd.cmd).to be_empty
      end

      it "still raises for errors other than a missing key" do
        Redis::TimeSeries.redis.with { |conn| conn.set("range_test_missing_key", "not a timeseries") }
        cmd = described_class.new(timeseries: missing_ts, start_time: Time.parse("2024-01-01"), end_time: Time.parse("2024-01-02"))

        expect { cmd.cmd }.to raise_error(Redis::CommandError)
      ensure
        Redis::TimeSeries.redis.with { |conn| conn.del("range_test_missing_key") }
      end
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
          expect(handle.data_command_count).to eq(3)
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
          expect(handle.data_command_count).to eq(2)
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
      # calendar_aggregation_loop narrows the window per month, with that month's exact length as
      # the bucket size, and had the same leak daily_aggregation did.
      it "restores its window and bucket size when a month raises" do
        from = Time.parse("2024-01-01")
        to = Time.parse("2024-04-01")
        range_cmd = described_class.new(timeseries: ts, start_time: from, end_time: to)
        range_cmd.aggregation = ["avg", 2_629_746_000]

        expect { range_cmd.enqueue(exploding_pipeline) }.to raise_error("boom")

        expect(range_cmd.start_time).to eq(from)
        expect(range_cmd.end_time).to eq(to)
        expect(range_cmd.options).to include(["AGGREGATION", "avg", 2_629_746_000])
      end

      # A window given in milliseconds, which RangeCmd accepts throughout, used to be compared
      # against a Time in the loop condition — Time#<=> hands an Integer to Date#<=>, which reads it
      # as a Julian day number, so the condition was always true and the loop spun forever inside
      # the Redis pipeline block, holding its connection.
      it "ends the loop for a window given in milliseconds" do
        from = Time.parse("2024-01-01")
        to = Time.parse("2024-04-01")
        ts.madd({ Time.parse("2024-01-15") => 10, Time.parse("2024-02-15") => 20 })
        as_msec = ->(time) { time.to_i * 1000 }

        numeric = described_class.new(timeseries: ts, start_time: as_msec.(from), end_time: as_msec.(to))
        numeric.aggregation = ["avg", 2_629_746_000]
        times = described_class.new(timeseries: ts, start_time: from, end_time: to)
        times.aggregation = ["avg", 2_629_746_000]

        result = Timeout.timeout(15) { numeric.cmd }

        expect(result.map { |sample| [sample.ts_msec, sample.value.to_s] })
          .to eq(times.cmd.map { |sample| [sample.ts_msec, sample.value.to_s] })
      end
    end

    # FILTER_BY_TS is sliced into 128-timestamp chunks, one command each, with @filter_by_ts
    # narrowed to the chunk in flight — the same leak the calendar paths had, and a caller that
    # retries after a dropped connection would then filter on one chunk of its timestamps.
    it "restores its filter_by_ts when a slice raises" do
      timestamps = (1..300).to_a
      range_cmd = described_class.new(timeseries: ts, start_time: Time.parse("2024-01-01"), end_time: Time.parse("2024-02-01"))
      range_cmd.filter_by_ts = timestamps

      expect { range_cmd.enqueue(exploding_pipeline) }.to raise_error("boom")

      expect(range_cmd.filter_by_ts).to eq(timestamps)
    end

    # enqueue_filtered_by_range points ALIGN at the window start for the duration of the slicing and
    # puts it back afterwards. Clipping the sub-ranges raises on the unbounded "-"/"+" default
    # window, which is before the restore had captured the old value — so the RangeCmd came out of
    # the failed call with no ALIGN at all, and the next query aligned on nothing.
    it "restores its align when clipping the sub-ranges raises" do
      range_cmd = described_class.new(timeseries: ts)
      range_cmd.aggregation = ["avg", 3_600_000]
      range_cmd.filter_by_range = [Time.parse("2024-01-01")..Time.parse("2024-01-02")]

      expect { range_cmd.cmd }.to raise_error(TypeError)

      expect(range_cmd.align).to eq("start")
    end

    context "with an aggregation duration of 1.day" do
      # Every example here depends on a zone that observes DST; CI runs UTC, where a
      # transition never happens and the assertions would pass without testing anything.
      around { |example| in_zone("Europe/Amsterdam") { example.run } }

      # One sample per hour, so a daily bucket's count is the number of hours it spans.
      def seed_hourly(ts, from, to)
        time = from
        while time <= to
          ts.add(1.0, (time.to_f * 1000).to_i)
          time += 3600
        end
      end

      def bucket_labels(samples)
        samples.map { |sample| Time.at(sample.ts_msec / 1000).strftime("%m-%d %H:%M") }
      end

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

      # The window that broke: buckets are 24h of elapsed time, so from a 14:37 start the
      # bucket after the autumn transition lands at 13:37 and every later one drifts with
      # it. The old correction restarted at local midnight and kept only rows matching the
      # first row's HH:MM, which silently dropped everything after the transition.
      it "keeps the bucket label stable when the window does not start at midnight" do
        seed_hourly(ts, Time.parse("2024-10-24"), Time.parse("2024-11-01"))

        range_cmd = described_class.new(timeseries: ts,
                                       start_time: Time.parse("2024-10-25 14:37"),
                                       end_time: Time.parse("2024-10-29 14:37"))
        range_cmd.aggregation = ["avg", 86_400_000]

        expect(bucket_labels(range_cmd.cmd)).to eq(["10-25 14:37", "10-26 14:37", "10-27 14:37", "10-28 14:37"])
      end

      it "keeps the bucket label stable across the spring transition too" do
        seed_hourly(ts, Time.parse("2024-03-29"), Time.parse("2024-04-03"))

        range_cmd = described_class.new(timeseries: ts,
                                       start_time: Time.parse("2024-03-30 09:15"),
                                       end_time: Time.parse("2024-04-02 09:15"))
        range_cmd.aggregation = ["avg", 86_400_000]

        expect(bucket_labels(range_cmd.cmd)).to eq(["03-30 09:15", "03-31 09:15", "04-01 09:15"])
      end

      it "gives the transition day its real length" do
        seed_hourly(ts, Time.parse("2024-10-26"), Time.parse("2024-10-29"))

        range_cmd = described_class.new(timeseries: ts,
                                       start_time: Time.parse("2024-10-26"),
                                       end_time: Time.parse("2024-10-28"))
        range_cmd.aggregation = ["count", 86_400_000]

        # The trailing 1 is the inclusive end_time instant in its own bucket, which any window
        # gets — a non-DST week of the same shape reads [24, 24, 1].
        expect(range_cmd.cmd.map { |sample| sample.value.to_i }).to eq([24, 25, 1])
      end

      # Splitting the transition days out must not turn into one command per day: a year
      # of daily buckets is two odd days plus the ordinary stretches between them.
      it "issues one command per run of ordinary days, not one per day" do
        pipeline = counting_pipeline

        range_cmd = described_class.new(timeseries: ts,
                                       start_time: Time.parse("2024-01-01 06:30"),
                                       end_time: Time.parse("2024-12-31 06:30"))
        range_cmd.aggregation = ["avg", 86_400_000]
        range_cmd.enqueue(pipeline)

        expect(pipeline.data_commands).to eq(5)
        expect(pipeline.probes).to eq(10)
      end

      # Each of the four below is a zone or a window the walker used to get wrong; all reproduced
      # against real Redis before being pinned here.

      # 02:00-02:59 does not exist on the spring-forward day, so the day that "ends" at 02:30 ends
      # 24 elapsed hours later at 03:30 instead. Correcting it back into the gap invented a second
      # short day and put every later label an hour out.
      it "does not invent a short day when the window sits in the spring-forward gap" do
        seed_hourly(ts, Time.parse("2024-03-28"), Time.parse("2024-04-04"))

        range_cmd = described_class.new(timeseries: ts,
                                       start_time: Time.parse("2024-03-29 02:30"),
                                       end_time: Time.parse("2024-04-02 02:30"))
        range_cmd.aggregation = ["count", 86_400_000]
        result = range_cmd.cmd

        # The last bucket is short only because the window ends inside it.
        expect(bucket_labels(result)).to eq(["03-29 02:30", "03-30 02:30", "03-31 03:30", "04-01 03:30"])
        expect(result.map { |sample| sample.value.to_i }).to eq([24, 24, 24, 23])
      end

      # A zero-length window enqueued one command before the window was ever split into runs, and
      # a caller asking for a single instant should still get its bucket rather than empty Samples.
      it "still enqueues one command for a zero-length window" do
        pipeline = counting_pipeline

        range_cmd = described_class.new(timeseries: ts,
                                       start_time: Time.parse("2024-06-01"),
                                       end_time: Time.parse("2024-06-01"))
        range_cmd.aggregation = ["avg", 86_400_000]
        range_cmd.enqueue(pipeline)

        expect(pipeline.data_commands).to eq(1)
      end

      context "in a zone whose offset moves by a whole day" do
        around { |example| in_zone("Pacific/Apia") { example.run } }

        # Samoa skipped 2011-12-30 entirely. Correcting the step by a 24-hour offset delta left the
        # grid standing still, and the walk hung inside the Redis pipeline block rather than ending.
        it "walks past the skipped day instead of hanging" do
          seed_hourly(ts, Time.parse("2011-12-27"), Time.parse("2012-01-03"))

          range_cmd = described_class.new(timeseries: ts,
                                         start_time: Time.parse("2011-12-28"),
                                         end_time: Time.parse("2012-01-02"))
          range_cmd.aggregation = ["count", 86_400_000]
          result = Timeout.timeout(15) { range_cmd.cmd }

          # 12-30 is absent because Samoa never had one.
          expect(bucket_labels(result)).to eq(["12-28 00:00", "12-29 00:00", "12-31 00:00", "01-01 00:00", "01-02 00:00"])
        end
      end

      context "in a zone with two transitions barely a week apart" do
        around { |example| in_zone("America/Boa_Vista") { example.run } }

        # Brazil started DST on 2000-10-08 and suspended it again on 2000-10-15. Any search that
        # skips ahead and compares offsets at the ends of a stride sees them cancel and finds
        # nothing, which is silently the behaviour this whole change exists to remove.
        it "finds both transitions" do
          seed_hourly(ts, Time.parse("2000-10-05"), Time.parse("2000-10-20"))

          range_cmd = described_class.new(timeseries: ts,
                                         start_time: Time.parse("2000-10-06"),
                                         end_time: Time.parse("2000-10-18"))
          range_cmd.aggregation = ["count", 86_400_000]

          result = range_cmd.cmd

          # DST starts at midnight on 10-08, so that day has no 00:00 and the grid carries on an
          # hour later; it ends on 10-15, making the day before it 25 hours long. A strided search
          # sees the two offsets cancel and reports neither.
          expect(bucket_labels(result).first(3)).to eq(["10-06 00:00", "10-07 00:00", "10-08 01:00"])
          expect(result.map { |sample| sample.value.to_i }).to eq([24, 24, 24, 24, 24, 24, 24, 24, 25, 24, 24, 24])
        end
      end

      # An operational schedule with no opening hours configured returns the whole query as one
      # range. Requiring a sub-range to sit inside a run dropped it at every run, so a daily read
      # over a window containing a transition came back completely empty.
      it "keeps a filter_by_range that spans the whole window when the window is split" do
        seed_hourly(ts, Time.parse("2024-10-24"), Time.parse("2024-10-30"))
        from = Time.parse("2024-10-25")
        to = Time.parse("2024-10-29")

        range_cmd = described_class.new(timeseries: ts, start_time: from, end_time: to)
        range_cmd.aggregation = ["count", 86_400_000]
        range_cmd.filter_by_range = [from..to]

        expect(range_cmd.cmd.size).to be > 0
      end

      # The other half of the same bug: a schedule that IS configured, but whose windows cross
      # midnight (a night baseload, 22:00 -> 06:00), straddles a run boundary on the transition day
      # and lost those nights rather than the whole read.
      it "keeps filter_by_range windows that cross a day boundary" do
        seed_hourly(ts, Time.parse("2024-10-24"), Time.parse("2024-10-30"))
        nights = (25..28).map { |day| Time.parse("2024-10-#{day} 22:00")..Time.parse("2024-10-#{day + 1} 06:00") }

        range_cmd = described_class.new(timeseries: ts,
                                       start_time: Time.parse("2024-10-25"),
                                       end_time: Time.parse("2024-10-29"))
        range_cmd.aggregation = ["count", 86_400_000]
        range_cmd.filter_by_range = nights
        counted = range_cmd.cmd.sum { |sample| sample.value.to_f.nan? ? 0 : sample.value.to_i }

        # 9 hourly samples a night, 10 on the night the clocks go back, and 3 on the last one
        # because the window ends inside it — clipped, not dropped.
        expect(counted).to eq(31)
      end

      # A Time serialises as `to_i * 1000`, so closing a run a whole second early left a
      # millisecond gap between one run and the next that no TS.RANGE covered.
      it "does not lose a sample in the millisecond before a run boundary" do
        boundary = Time.parse("2024-10-27")
        ts.add(1.0, (boundary.to_f * 1000).to_i - 1)
        ts.add(1.0, (boundary.to_f * 1000).to_i)

        range_cmd = described_class.new(timeseries: ts,
                                       start_time: Time.parse("2024-10-25"),
                                       end_time: Time.parse("2024-10-29"))
        range_cmd.aggregation = ["count", 86_400_000]

        expect(range_cmd.cmd.sum { |sample| sample.value.to_f.nan? ? 0 : sample.value.to_i }).to eq(2)
      end

      # The same millisecond, through filter_by_range. Two layers threw it away independently: the
      # `start_time`/`end_time` readers floored a millisecond bound to the whole second, so the clip
      # landed a second early, and Client#cmd_with_redis serialises a Time as `to_i * 1000`, so
      # passing the clipped bound on as a Time truncated it again.
      it "does not lose that millisecond when the read is filtered by range" do
        from = Time.parse("2024-10-25")
        to = Time.parse("2024-10-29")
        boundary = Time.parse("2024-10-27")
        [-1, 0, 1].each { |offset| ts.add(1.0, (boundary.to_f * 1000).to_i + offset) }

        range_cmd = described_class.new(timeseries: ts, start_time: from, end_time: to)
        range_cmd.aggregation = ["count", 86_400_000]
        range_cmd.filter_by_range = [from..to]

        expect(range_cmd.cmd.sum { |sample| sample.value.to_f.nan? ? 0 : sample.value.to_i }).to eq(3)
      end

      # A run closes at `msec(grid) - 1`, so the next one has to open at exactly `msec(grid)`.
      # Handed on as a Time it was serialised `to_i * 1000`, reopening the run at the top of the
      # second: with a grid carrying a fractional second the two commands overlapped by up to 999 ms
      # and every sample in that gap was returned by both. Two samples came back counted as four.
      it "does not count a sample twice when the window starts mid-second" do
        start_time = Time.at(Time.parse("2024-10-25 14:37:00").to_f + 0.5)
        end_time = Time.at(Time.parse("2024-10-29 14:37:00").to_f + 0.5)
        # One sample inside each run boundary's overlap window (<second>.200).
        [Time.parse("2024-10-26 14:37:00"), Time.parse("2024-10-27 14:37:00")].each do |boundary|
          ts.add(1.0, (boundary.to_f * 1000).to_i + 200)
        end

        range_cmd = described_class.new(timeseries: ts, start_time: start_time, end_time: end_time)
        range_cmd.aggregation = ["count", 86_400_000]
        rows = range_cmd.cmd.reject { |sample| sample.value.to_f.nan? }

        expect(rows.sum { |sample| sample.value.to_i }).to eq(2)
        expect(rows.map(&:ts_msec).uniq.size).to eq(rows.size)
      end

      # The window accepts millisecond Integers, so a caller working in milliseconds hands
      # filter_by_range Integers too. Clipping compares the two, and against the Time-valued window
      # that raised `comparison of Integer with Time failed` — where the containment check it
      # replaced had compared them raw and worked.
      it "clips a filter_by_range given in milliseconds, to the same answer as Times" do
        from = Time.parse("2024-10-25")
        to = Time.parse("2024-10-29")
        seed_hourly(ts, Time.parse("2024-10-24"), Time.parse("2024-10-30"))
        as_msec = ->(time) { (time.to_f * 1000).to_i }

        numeric = described_class.new(timeseries: ts, start_time: as_msec.(from), end_time: as_msec.(to))
        numeric.aggregation = ["count", 86_400_000]
        numeric.filter_by_range = [as_msec.(from)..as_msec.(to)]

        times = described_class.new(timeseries: ts, start_time: from, end_time: to)
        times.aggregation = ["count", 86_400_000]
        times.filter_by_range = [from..to]

        expect { numeric.cmd }.not_to raise_error
        expect(numeric.cmd.map { |sample| [sample.ts_msec, sample.value.to_s] })
          .to eq(times.cmd.map { |sample| [sample.ts_msec, sample.value.to_s] })
      end

      # Pinned so the next editor meets the trade-off deliberately: the rows sum correctly, but two
      # partial `avg`s cannot be recombined, and the alternative is dropping one of the two
      # intervals, which is the data loss the clipping removed.
      it "splits a sub-range that straddles a run boundary into two rows for one bucket" do
        seed_hourly(ts, Time.parse("2024-10-24"), Time.parse("2024-10-31"))
        # 08:00-16:00 straddles a grid that starts at 14:37.
        days = (25..29).map { |day| Time.parse("2024-10-#{day} 08:00")..Time.parse("2024-10-#{day} 16:00") }

        range_cmd = described_class.new(timeseries: ts,
                                       start_time: Time.parse("2024-10-25 14:37"),
                                       end_time: Time.parse("2024-10-30 14:37"))
        range_cmd.aggregation = ["count", 86_400_000]
        range_cmd.filter_by_range = days
        rows = range_cmd.cmd.reject { |sample| sample.value.to_f.nan? }
        stamps = rows.map(&:ts_msec)

        expect(stamps.size).to be > stamps.uniq.size
        # Nothing is lost or double-counted: 9 hourly samples in each full 08:00-16:00 window, and
        # 2 in the first, which the query's own 14:37 start clips.
        expect(rows.sum { |sample| sample.value.to_i }).to eq(2 + (9 * 4))
      end

      # The window ends on a day boundary, so the final instant falls outside the transition run.
      # It used to be dropped, while the same window in a week with no transition kept it.
      it "keeps a sample sitting exactly on the window end" do
        seed_hourly(ts, Time.parse("2024-10-25"), Time.parse("2024-10-27"))
        finish = Time.parse("2024-10-28")
        ts.add(1.0, (finish.to_f * 1000).to_i)

        range_cmd = described_class.new(timeseries: ts, start_time: Time.parse("2024-10-26"), end_time: finish)
        range_cmd.aggregation = ["count", 86_400_000]
        result = range_cmd.cmd

        expect(bucket_labels(result).last).to eq("10-28 00:00")
        expect(result.last.value.to_i).to eq(1)
      end

      # The window is split into several TS.RANGEs, each with its own start, end and bucket size.
      # A raise part-way through used to leave the RangeCmd pointing at one sub-window with a
      # 25-hour aggregation, so a caller that retried it queried the wrong range.
      it "restores its window and bucket size when a run raises" do
        from = Time.parse("2024-10-25")
        to = Time.parse("2024-10-29")
        range_cmd = described_class.new(timeseries: ts, start_time: from, end_time: to)
        range_cmd.aggregation = ["avg", 86_400_000]

        expect { range_cmd.enqueue(exploding_pipeline) }.to raise_error("boom")

        expect(range_cmd.start_time).to eq(from)
        expect(range_cmd.end_time).to eq(to)
        expect(range_cmd.options).to include(["AGGREGATION", "avg", 86_400_000])
      end

      # Every bucket is a true 24 hours, but where the clock jumps AT midnight the label moves onto
      # the next date and stays there — so past the transition the labels are no longer dates. Pinned
      # so a change to the walker has to decide about it rather than drift into it.
      context "in a zone whose spring-forward is at midnight" do
        around { |example| in_zone("America/Santiago") { example.run } }

        it "keeps 24-hour buckets but shifts the label for the rest of the window" do
          seed_hourly(ts, Time.parse("2024-09-05"), Time.parse("2024-09-13"))

          range_cmd = described_class.new(timeseries: ts,
                                         start_time: Time.parse("2024-09-06"),
                                         end_time: Time.parse("2024-09-11"))
          range_cmd.aggregation = ["count", 86_400_000]
          result = range_cmd.cmd

          expect(result.map { |sample| sample.value.to_i }).to all(eq(24))
          expect(bucket_labels(result)).to eq(["09-06 00:00", "09-07 00:00", "09-08 01:00", "09-09 01:00", "09-10 01:00"])
        end
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
      expect(handle.data_command_count).to eq(3)
      expect(handle.queried_timestamps.size).to eq(3)
    end
  end
end
