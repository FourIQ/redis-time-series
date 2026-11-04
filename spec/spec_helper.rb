require 'simplecov'
#SimpleCov.start { add_filter '/spec/' }

require 'bundler/setup'
require 'active_support'
require 'active_support/core_ext/numeric/time'
require 'active_support/testing/time_helpers'
require 'pry'
require 'redis'
require 'timeout'
require 'securerandom'
require 'redis-time-series'

REDIS_PORT = ENV['REDIS_PORT'] || 9000
REDIS_HOST = ENV['REDIS_HOST'] || '127.0.0.1'
# nil (not "") when unset: redis-client sends AUTH for any non-nil password,
# which errors against a passwordless Redis.
REDIS_PASSWORD = ENV['REDIS_PASSWORD'].to_s.empty? ? nil : ENV['REDIS_PASSWORD']

# DST behaviour can only be exercised in a zone that observes it, and CI runs UTC.
module ZoneHelpers
  def in_zone(zone)
    original = ENV["TZ"]
    ENV["TZ"] = zone
    yield
  ensure
    ENV["TZ"] = original
  end
end

# db 13 is shared with other suites (DE, PE's Redis::Objects), so it is never flushed. Instead
# every key and label value this run writes is its own: a stray key can't match, concurrent runs
# can't collide, and cleanup deletes only what this run created.
SPEC_RUN = "#{Process.pid}-#{SecureRandom.hex(3)}"
SPEC_NAMESPACE = "rts_spec:#{SPEC_RUN}:"

module RedisHelpers
  def redis
    @redis ||= ConnectionPool.new(size: 25, timeout: 50) { Redis.new(host: REDIS_HOST, port: REDIS_PORT, password: REDIS_PASSWORD, db: 13) }
  end

  def spec_key(name)
    "#{SPEC_NAMESPACE}#{name}"
  end

  def spec_label(value)
    "#{value}-#{SPEC_RUN}"
  end
end

RSpec.configure do |config|
  config.disable_monkey_patching!
  config.example_status_persistence_file_path = '.rspec_status'
  config.filter_run_when_matching :focus

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.include RedisHelpers
  config.include ZoneHelpers
  config.include ActiveSupport::Testing::TimeHelpers

  config.before { Redis::TimeSeries.redis = redis }
  config.after(:suite) do
    conn = Redis.new(host: REDIS_HOST, port: REDIS_PORT, password: REDIS_PASSWORD, db: 13)
    conn.scan_each(match: "#{SPEC_NAMESPACE}*", count: 1000).each_slice(500) { |keys| conn.del(*keys) }
  ensure
    conn&.close
  end
end

RSpec::Matchers.define :issue_command do |expected|
  supports_block_expectations

  match do |actual|
    @commands = []
    allow_any_instance_of(Redis).to receive(:call).and_wrap_original do |redis, *args|
      @commands << args.join(' ')
      redis.call(*args)
    end

    allow_any_instance_of(Redis::PipelinedConnection).to receive(:call).and_wrap_original do |redis, *args|
      @commands << args.join(' ')
      redis.call(*args)
    end

    actual.call
    expect(@commands).to include(expected)
  end

  failure_message do |actual|
    "expected command #{expected}\n" \
      "received commands:\n" \
      "  #{@commands.join("\n  ")}"
  end
end
