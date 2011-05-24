require 'active_record'

class ActiveRecord::Base
  def self.load_for_delayed_job(id)
    if id
      find(id)
    else
      super
    end
  end
  
  def dump_for_delayed_job
    "#{self.class};#{id}"
  end
end

module Delayed
  module Backend
    module ActiveRecord
      # A job object that is persisted to the database.
      # Contains the work object as a YAML field.
      class Job < ::ActiveRecord::Base
        include Delayed::Backend::Base
        set_table_name :delayed_jobs
        
        before_save :set_default_run_at

        scope :ready_to_run, lambda {|worker_name, max_run_time|
          where(['(run_at <= ? AND (locked_at IS NULL OR locked_at < ?) OR locked_by = ?) AND failed_at IS NULL', db_time_now, db_time_now - max_run_time, worker_name])
        }
        scope :by_priority, order('priority ASC, run_at ASC')
        scope :existing, lambda { |obj_class, obj_id, method| where(:performable_id => obj_id, :performable_type => obj_class.to_s, :performable_method => method.to_s) }
        
        before_save :set_performable_info
        before_save :set_site_keyname
        after_initialize :set_site
        
        before_create :reschedule_if_found
        
        def reschedule_if_found
          puts "self: #{self.inspect}"
          if !self.payload_object.respond_to?(:perform)
            raise ArgumentError, 'Cannot enqueue items which do not respond to perform'
          end
          
          puts "run_at: #{run_at.inspect}"
                    
          puts "OBJECT: #{payload_object.inspect} ARGS: #{payload_object.args.inspect}"
          if self.payload_object.respond_to?(:args) && self.payload_object.args.is_a?(Array) && self.payload_object.args[0].is_a?(Hash) && self.payload_object.args[0][:reschedule_if_found]
            self.payload_object.args[0].delete :reschedule_if_found
            # did we just blank out the hash? make it nil if so
            if self.payload_object.args[0].blank?
              self.payload_object.args.delete_at(0)
            end
            puts "OBJECT NOW: #{payload_object.inspect}"
            # should we reschedule an existing job, or create it?
            # hack in here for make_payment calls -- those will be 
            if run_at && self.payload_object.is_a?(Delayed::PerformableMethod) && Delayed::PerformableMethod::STRING_FORMAT === self.payload_object.object
              klass = $1
              id = $2
              if id.present? && matching = self.class.existing(klass, id, self.payload_object.method)
                if matching.length > 0
                  puts "JUST RESHEDULING #{klass} #{id}"
                  matching.each{|x| x.reschedule!(run_at) }
                  return false
                end
              end
            end
          end
          
          true
        end
        
        def set_performable_info
          if payload_object && payload_object.object
            # do we match the format 
            if Delayed::PerformableMethod::STRING_FORMAT === payload_object.object
              klass = $1
              id = $2
              if id.present?
                self.performable_id = id.to_i
                self.performable_type = klass.to_s
                self.performable_method = payload_object.method.to_s
              end
            end
          end
          true
        end
        
        def set_site_keyname
          self.site_keyname = Site.keyname.to_s
        end
        
        def set_site
          Site.set_keyname(self.site_keyname) if !new_record?
        end
        
        def reschedule!(time)
          update_attribute(:run_at, time) unless time.to_s(&:db) == run_at.to_s(&:db)
        end
        
        def self.after_fork
          ::ActiveRecord::Base.connection.reconnect!
        end

        # When a worker is exiting, make sure we don't have any locked jobs.
        def self.clear_locks!(worker_name)
          update_all("locked_by = null, locked_at = null", ["locked_by = ?", worker_name])
        end

        # Find a few candidate jobs to run (in case some immediately get locked by others).
        def self.find_available(worker_name, limit = 5, max_run_time = Worker.max_run_time)
          scope = self.ready_to_run(worker_name, max_run_time)
          scope = scope.scoped(:conditions => ['priority >= ?', Worker.min_priority]) if Worker.min_priority
          scope = scope.scoped(:conditions => ['priority <= ?', Worker.max_priority]) if Worker.max_priority
      
          ::ActiveRecord::Base.silence do
            scope.by_priority.all(:limit => limit)
          end
        end

        # Lock this job for this worker.
        # Returns true if we have the lock, false otherwise.
        def lock_exclusively!(max_run_time, worker)
          now = self.class.db_time_now
          affected_rows = if locked_by != worker
            # We don't own this job so we will update the locked_by name and the locked_at
            self.class.update_all(["locked_at = ?, locked_by = ?", now, worker], ["id = ? and (locked_at is null or locked_at < ?) and (run_at <= ?)", id, (now - max_run_time.to_i), now])
          else
            # We already own this job, this may happen if the job queue crashes.
            # Simply resume and update the locked_at
            self.class.update_all(["locked_at = ?", now], ["id = ? and locked_by = ?", id, worker])
          end
          if affected_rows == 1
            self.locked_at = now
            self.locked_by = worker
            self.locked_at_will_change!
            self.locked_by_will_change!
            return true
          else
            return false
          end
        end

        # Get the current time (GMT or local depending on DB)
        # Note: This does not ping the DB to get the time, so all your clients
        # must have syncronized clocks.
        def self.db_time_now
          if Time.zone
            Time.zone.now
          elsif ::ActiveRecord::Base.default_timezone == :utc
            Time.now.utc
          else
            Time.now
          end
        end

      end
    end
  end
end
