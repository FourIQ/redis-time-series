# frozen_string_literal: true

require "spec_helper"

# redis-rb 5 negotiates RESP2 and redis-rb 6 RESP3, which answers this command with a map. Both
# fixtures are replies from one Redis 8.10.1 server, so the parser is held to one result either way.
RSpec.describe Redis::TimeSeries::Multi do
  let(:resp2) { [["m1", [["foo", "bar"]], [[1_700_000_000_000, "1"], [1_700_000_060_000, "2"]]]] }
  let(:resp3) do
    { "m1" => [{ "foo" => "bar" }, { "aggregators" => [] }, [[1_700_000_000_000, 1.0], [1_700_000_060_000, 2.0]]] }
  end

  def flat(multi) = multi.map { |result| [result.series.key, result.labels, result.samples.map { |s| [s.ts_msec, s.value] }] }

  it "parses a RESP3 map to the same result as a RESP2 array" do
    expect(flat(described_class.new(resp3))).to eq(flat(described_class.new(resp2)))
  end

  # RESP2 sends no labels as [] and RESP3 as {}; callers only ever saw the first.
  it "keeps unrequested labels as an empty array" do
    multi = described_class.new("m1" => [{}, { "aggregators" => [] }, [[1_700_000_000_000, 1.0]]])

    expect(multi.first.labels).to eq([])
  end
end
