module Delayed
  module Backend
    class DeserializationError < StandardError
    end

    module Base
      def self.included(base)
        base.extend ClassMethods
      end
      
      module ClassMethods
        # Add a job to the queue
        def enqueue(*args)
          object = args.shift
          priority = args.first || Delayed::Worker.default_priority
          
          unless object.respond_to?(:perform)
            raise ArgumentError, 'Cannot enqueue items which do not respond to perform'
          end
          
          run_at = args[1]
                    
          # puts "OBJECT: #{object.inspect} ARGS: #{object.args.inspect}"
          if object.respond_to?(:args) && object.args.is_a?(Array) && object.args[0].is_a?(Hash) && object.args[0][:reschedule_if_found]
            object.args[0].delete :reschedule_if_found
            # did we just blank out the hash? make it nil if so
            if object.args[0].blank?
              object.args.delete_at(0)
            end
            # puts "OBJECT NOW: #{object.inspect}"
            # should we reschedule an existing job, or create it?
            # hack in here for make_payment calls -- those will be 
            if run_at && object.is_a?(Delayed::PerformableMethod) && Delayed::PerformableMethod::STRING_FORMAT === object.object
              klass = $1
              id = $2
              if id.present? && matching = existing(klass, id, object.method)
                if matching.length > 0
                  puts "JUST RESHEDULING #{klass} #{id}"
                  matching.each{|x| x.reschedule!(run_at) }
                  return matching
                end
              end
            end
          end

          self.create(:payload_object => object, :priority => priority.to_i, :run_at => run_at)
        end

        def reserve(worker, max_run_time = Worker.max_run_time)
          # We get up to 5 jobs from the db. In case we cannot get exclusive access to a job we try the next.
          # this leads to a more even distribution of jobs across the worker processes
          find_available(worker.name, 5, max_run_time).detect do |job|
            job.lock_exclusively!(max_run_time, worker.name)
          end
        end

        # Hook method that is called before a new worker is forked
        def before_fork
        end
        
        # Hook method that is called after a new worker is forked
        def after_fork
        end
        
        def work_off(num = 100)
          warn "[DEPRECATION] `Delayed::Job.work_off` is deprecated. Use `Delayed::Worker.new.work_off instead."
          Delayed::Worker.new.work_off(num)
        end
      end
      
      ParseObjectFromYaml = /\!ruby\/\w+\:([^\s]+)/

      def failed?
        failed_at
      end
      alias_method :failed, :failed?

      def name
        @name ||= begin
          payload = payload_object
          if payload.respond_to?(:display_name)
            payload.display_name
          else
            payload.class.name
          end
        end
      end

      def payload_object=(object)
        self['handler'] = object.to_yaml
      end
      
      def payload_object
        @payload_object ||= deserialize(self['handler'])
      end

      # Moved into its own method so that new_relic can trace it.
      def invoke_job
        payload_object.perform
      end
      
      # Unlock this job (note: not saved to DB)
      def unlock
        self.locked_at    = nil
        self.locked_by    = nil
      end
    
      def reschedule_at
        payload_object.respond_to?(:reschedule_at) ? 
          payload_object.reschedule_at(self.class.db_time_now, attempts) :
          self.class.db_time_now + (attempts ** 4) + 5
      end

      def max_attempts
        payload_object.max_attempts if payload_object.respond_to?(:max_attempts)
      end
      
    private

      def deserialize(source)
        handler = YAML.load(source) rescue nil

        unless handler.respond_to?(:perform)
          if handler.nil? && source =~ ParseObjectFromYaml
            handler_class = $1
          end
          attempt_to_load(handler_class || handler.class)
          handler = YAML.load(source)
        end

        return handler if handler.respond_to?(:perform)

        raise DeserializationError,
          'Job failed to load: Unknown handler. Try to manually require the appropriate file.'
      rescue TypeError, LoadError, NameError => e
        raise DeserializationError,
          "Job failed to load: #{e.message}. Try to manually require the required file."
      end

      # Constantize the object so that ActiveSupport can attempt
      # its auto loading magic. Will raise LoadError if not successful.
      def attempt_to_load(klass)
         klass.constantize
      end
      
    protected

      def set_default_run_at
        self.run_at ||= self.class.db_time_now
      end    

    end
  end
end
