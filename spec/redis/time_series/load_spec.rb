# frozen_string_literal: true

require "rbconfig"

# spec_helper loads redis first, so only a fresh process can see what the gem itself requires.
RSpec.describe "require 'redis-time-series'" do
  it "loads on its own" do
    lib = File.expand_path("../../../lib", __dir__)
    ok = system(RbConfig.ruby, "-I", lib, "-e", "require 'redis-time-series'", out: File::NULL, err: File::NULL)

    expect(ok).to be(true)
  end
end
