# frozen_string_literal: true

require "spec_helper"

# redis-rb 5 negotiates RESP2 and redis-rb 6 RESP3, which answers this command with a map. Both
# fixtures are replies from one Redis 8.10.1 server, so the parser is held to one result either way.
RSpec.describe Redis::TimeSeries::Info do
  let(:series) { Redis::TimeSeries.new("src") }

  let(:resp2) do
    ["totalSamples", 1, "memoryUsage", 4472, "firstTimestamp", 1_700_000_000_000,
     "lastTimestamp", 1_700_000_000_000, "retentionTime", 0, "chunkCount", 1, "chunkSize", 4096,
     "chunkType", "compressed", "duplicatePolicy", "block",
     "labels", [["foo", "bar"], ["baz", "1"]], "sourceKey", nil,
     "rules", [["dst", 60_000, "AVG", 0]], "ignoreMaxTimeDiff", 0, "ignoreMaxValDiff", "0"]
  end
  let(:resp3) do
    { "totalSamples" => 1, "memoryUsage" => 4472, "firstTimestamp" => 1_700_000_000_000,
      "lastTimestamp" => 1_700_000_000_000, "retentionTime" => 0, "chunkCount" => 1, "chunkSize" => 4096,
      "chunkType" => "compressed", "duplicatePolicy" => "block",
      "labels" => { "foo" => "bar", "baz" => "1" }, "sourceKey" => nil,
      "rules" => { "dst" => [60_000, "AVG", 0] }, "ignoreMaxTimeDiff" => 0, "ignoreMaxValDiff" => 0.0 }
  end

  def rules(info)
    info.rules.map { |rule| [rule.destination_key, rule.aggregation.type, rule.aggregation.duration] }
  end

  it "parses a RESP3 map to the same info as a RESP2 array" do
    from2 = described_class.parse(series: series, data: resp2)
    from3 = described_class.parse(series: series, data: resp3)

    expect(from3.to_h.except(:rules, :series)).to eq(from2.to_h.except(:rules, :series))
    expect(rules(from3)).to eq(rules(from2))
  end

  it "reads labels, rules and the policy out of either shape" do
    info = described_class.parse(series: series, data: resp3)

    expect(info.labels).to eq("foo" => "bar", "baz" => 1)
    expect(rules(info)).to eq([["dst", "avg", 60_000]])
    expect(info.duplicate_policy).to be_block
  end
end
