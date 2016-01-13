require 'json'

module Resque
  module Plugins
    module LockedJob
      def self.included(base)
        base.class_eval do
          extend ClassMethods

          @__is_locked_job = true
          @locked_max_parallel_number = nil
        end
      end

      module ClassMethods
        @__has_lock = false

        def lock_key *args
          args.to_json
        end

        def before_reserve_resque_lock *args
          max_parallel_number =
            instance_variable_get(:@max_parallel_number) ||
            (respond_to?(:max_parallel_number) && klass.max_parallel_number) ||
            1
          key = lock_key *args
          if Resque.redis.incr("resque-lock:#{key}").to_i <= max_parallel_number
            @__has_lock = true
            true
          else
            Resque.redis.decr "resque-lock:#{key}"
            @__has_lock = false
            false
          end
        end

        def after_perform_resque_lock *args
          key = lock_key *args
          Resque.redis.decr "resque-lock:#{key}"
        end

        def after_requeue_resque_lock *args
          if @__has_lock
            after_perform_resque_lock *args
          end
        end

        def on_failure_resque_lock _exception, *args
          after_perform_resque_lock *args
        end
      end
    end
  end
end
