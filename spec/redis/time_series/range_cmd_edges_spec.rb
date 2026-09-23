# frozen_string_literal: true

require "spec_helper"

# With EMPTY, Redis 8.2 reported buckets from the first one holding data to the last; 8.10 fills empty
# ones out to the window's edges. These pin the 8.2 behaviour on whichever server the suite runs.
RSpec.describe Redis::TimeSeries::RangeCmd do
  let(:key) { "range_edges_test" }

  subject(:ts) { Redis::TimeSeries.create(key) }

  around { |example| in_zone("Europe/Amsterdam") { example.run } }
  after { Redis::TimeSeries.redis.with { |conn| conn.del(key) } }

  # One sample on every hour, valued 1, 2, 3, ... so a bucket's value says which samples it held.
  before do
    time = Time.parse("2025-10-01")
    value = 0
    while time <= Time.parse("2025-10-12")
      ts.add(value += 1, (time.to_f * 1000).to_i)
      time += 3600
    end
  end

  def read(from, to, aggregation, duration, reverse: false, filter_by_value: nil)
    cmd = described_class.new(timeseries: ts, start_time: Time.parse(from), end_time: Time.parse(to))
    cmd.revrange if reverse
    cmd.filter_by_value = filter_by_value if filter_by_value
    cmd.aggregation = [aggregation, duration]
    cmd.cmd
  end

  def labels(samples) = samples.map { |sample| Time.at(sample.ts_msec / 1000).strftime("%m-%d %H:%M") }

  it "starts and ends at the buckets holding the window's first and last sample" do
    result = read("2025-10-02 14:37", "2025-10-10 14:37", "avg", 900_000)

    expect(labels(result).values_at(0, -1)).to eq(["10-02 14:52", "10-10 13:52"])
  end

  # These three fill an empty bucket with something that reads as data: 0, or the previous value.
  %w[sum count last].each do |aggregation|
    it "adds no bucket past the last sample for #{aggregation}" do
      expect(read("2025-10-02 14:37", "2025-10-10 14:37", aggregation, 86_400_000).size).to eq(8)
    end
  end

  # Inclusive end: period totals and cumulative-meter differences need the reading on end_time.
  it "keeps the bucket of a sample sitting exactly on the window end" do
    result = read("2025-10-02 00:00", "2025-10-10 00:00", "sum", 86_400_000)

    expect(labels(result).last).to eq("10-10 00:00")
    expect(result.last.value.to_i).to eq(217)
  end

  it "returns nothing for a window between two samples" do
    expect(read("2025-10-05 14:05", "2025-10-05 14:55", "sum", 900_000)).to be_empty
  end

  it "gives a zero-length window its bucket only when a sample sits on it" do
    expect(read("2025-10-05 14:00", "2025-10-05 14:00", "avg", 3_600_000).size).to eq(1)
    expect(read("2025-10-05 14:37", "2025-10-05 14:37", "avg", 3_600_000)).to be_empty
  end

  # The shape TimeseriesQueries#aggregation_for_period reads a period total with: one bucket, a
  # millisecond wider than its window. An empty period must come back as no bucket, not NaN.
  it "returns an empty period as no bucket" do
    from = Time.parse("2025-10-05 14:05")
    to = Time.parse("2025-10-05 14:55")
    cmd = described_class.new(timeseries: ts, start_time: from, end_time: to)
    cmd.aggregation = ["avg", ((to.to_i - from.to_i) * 1000) + 1]

    expect(cmd.cmd).to be_empty
  end

  it "trims to the samples that pass the read's own value filter" do
    result = read("2025-10-01 00:00", "2025-10-10 00:00", "avg", 3_600_000, filter_by_value: [0, 50])

    expect(labels(result).last).to eq("10-03 01:00")
  end

  it "reads the same buckets in reverse" do
    forward = read("2025-10-02 14:37", "2025-10-10 14:37", "sum", 900_000)
    reverse = read("2025-10-02 14:37", "2025-10-10 14:37", "sum", 900_000, reverse: true)

    expect(reverse.map { |sample| [sample.ts_msec, sample.value] })
      .to eq(forward.map { |sample| [sample.ts_msec, sample.value] }.reverse)
  end

  # `twa` interpolates across bucket edges, so it is left as Redis returns it.
  it "sends no probes for twa" do
    cmd = described_class.new(timeseries: ts, start_time: Time.parse("2025-10-02 14:37"),
                                              end_time: Time.parse("2025-10-10 14:37"))
    cmd.aggregation = ["twa", 86_400_000]
    handle = nil
    Redis::TimeSeries.redis.with { |conn| conn.pipelined { |pipeline| handle = cmd.enqueue(pipeline) } }

    expect(handle.data_command_count).to eq(handle.command_count)
  end
end
