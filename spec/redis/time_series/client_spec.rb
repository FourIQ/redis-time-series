# frozen_string_literal: true

require "spec_helper"

RSpec.describe Redis::TimeSeries::Client do
  describe ".wire" do
    it "sends a Time as its milliseconds, floored" do
      expect(described_class.wire(Time.at(Rational(12_345_678, 10_000)))).to eq(1_234_567)
    end

    it "sends a TimeWithZone the same way" do
      time = ActiveSupport::TimeWithZone.new(Time.at(Rational(12_345_678, 10_000)).utc, TZInfo::Timezone.get("Etc/UTC"))

      expect(described_class.wire(time)).to eq(1_234_567)
    end

    it "sends anything else as its string" do
      expect(described_class.wire(1_234_567)).to eq("1234567")
    end
  end

  # end_of_day is 23:59:59.999999; sent as whole seconds it stopped at 23:59:59.000 and dropped the
  # last second of every day a caller asked for.
  describe "a range bounded by end_of_day" do
    let(:key) { spec_key("wire_test") }

    subject(:ts) { Redis::TimeSeries.create(key) }

    after { Redis::TimeSeries.redis.with { |conn| conn.del(key) } }

    around { |example| in_zone("Europe/Amsterdam") { example.run } }

    # The daily path builds its own run bounds; rounding end_of_day put the last one on the next midnight.
    it "keeps a daily read to the days it names" do
      (0..3).each { |day| ts.add(1, ((Time.parse("2025-10-05") + day.days).to_f * 1000).to_i) }

      result = ts.range(Time.parse("2025-10-05")..Time.parse("2025-10-07").end_of_day, aggregation: [:sum, 86_400_000])

      expect(result.size).to eq(3)
    end

    it "reaches a sample in the day's last second" do
      day = Time.parse("2025-10-05")
      ts.add(1, ((day.end_of_day - 0.5).to_f * 1000).to_i)

      expect(ts.range(day.beginning_of_day..day.end_of_day).size).to eq(1)
    end
  end
end
