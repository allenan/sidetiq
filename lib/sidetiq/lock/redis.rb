module Sidetiq
  module Lock
    class Redis
      include Logging

      attr_reader :key, :timeout

      def self.all
        Sidekiq.redis do |redis|
          redis.keys("sidetiq:*:lock").map do |key|
            new(key)
          end
        end
      end

      def initialize(key, timeout = Sidetiq.config.lock_expire)
        @key = extract_key(key)
        @millisecond_precision = Sidekiq.redis do |redis|
          redis.info['redis_version'] >= '2.6'
        end
        @timeout = if millisecond_precision?
                     timeout
                   else
                     timeout > 1000 ? timeout / 1000 : 1
                   end
      end

      def synchronize
        Sidekiq.redis do |redis|
          acquired = lock

          if acquired
            debug "Lock: #{key}"

            begin
              yield redis
            ensure
              unlock
              debug "Unlock: #{key}"
            end
          end
        end
      end

      def stale?
        pttl = meta_data.pttl

        # Consider PTTL of -1 (never set) and larger than the
        # configured lock_expire as invalid. Locks with timestamps
        # older than 1 minute are also considered stale.
        pttl < 0 || pttl >= Sidetiq.config.lock_expire ||
          meta_data.timestamp < (Sidetiq.clock.gettime.to_i - 60)
      end

      def meta_data
        @meta_data ||= Sidekiq.redis do |redis|
          MetaData.from_json(redis.get(key))
        end
      end

      def millisecond_precision?
        !!@millisecond_precision
      end

      def lock
        Sidekiq.redis do |redis|
          acquired = false

          watch(redis, key) do
            if !redis.exists(key)
              acquired = !!redis.multi do |multi|
                meta = MetaData.for_new_lock(key)

                if millisecond_precision?
                  multi.psetex(key, timeout, meta.to_json)
                else
                  multi.setex(key, timeout, meta.to_json)
                end
              end
            end
          end

          acquired
        end
      end

      def unlock
        Sidekiq.redis do |redis|
          watch(redis, key) do
            if meta_data.owner == Sidetiq::Lock::MetaData::OWNER
              redis.multi do |multi|
                multi.del(key)
              end

              true
            else
              false
            end
          end
        end
      end

      def unlock!
        Sidekiq.redis do |redis|
          redis.del(key)
        end
      end

      private

      def extract_key(key)
        case key
        when Class
          "sidetiq:#{key.name}:lock"
        when String
          key.match(/sidetiq:(.+):lock/) ? key : "sidetiq:#{key}:lock"
        end
      end

      def watch(redis, *args)
        redis.watch(*args)

        begin
          yield
        ensure
          redis.unwatch
        end
      end
    end
  end
end
