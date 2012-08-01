module SalesforceBulk
  class Job
    attr_accessor :client

    attr_reader :concurrency_mode
    attr_reader :external_id_field_name
    attr_reader :data

    @@fields = [:id, :operation, :object, :createdById, :state, :createdDate, 
      :systemModstamp, :externalIdFieldName, :concurrencyMode, :contentType, 
      :numberBatchesQueued, :numberBatchesInProgress, :numberBatchesCompleted, 
      :numberBatchesFailed, :totalBatches, :retries, :numberRecordsProcessed, 
      :numberRecordsFailed, :totalProcessingTime, :apiActiveProcessingTime, 
      :apexProcessingTime, :apiVersion]
    
    @@valid_operations = [:delete, :insert, :update, :upsert, :query]
    @@valid_concurrency_modes = ['Parallel', 'Serial']


    @@fields.each do |field|
      attr_reader field.to_s.underscore.to_sym
    end

    def self.valid_operation? operation
      @@valid_operations.include?(operation)
    end

    def self.valid_concurrency_mode? mode
      @@valid_concurrency_modes.include?(concurrency_mode)
    end

    def new_from_xml xml_data, client = nil
      job = Job.new
      job.update(xml_data)
      job.client = client
    end

    def update xml_data
      #Check fields
      xml_data.assert_valid_keys(@@fields)

      #Assign object
      @xml_data = xml_data

      #Mass assign the defaults
      @@fields.each do |field|
        instance_variable_set(field, xml_data[field])
      end

      #Special cases and data formats
      @created_date = DateTime.parse(xml_data['createdDate'])
      @system_modstamp = DateTime.parse(xml_data['systemModstamp'])

      @retries = xml_data['retries'].to_i
      @api_version = xml_data['apiVersion'].to_i
      @number_batches_queued = xml_data['numberBatchesQueued'].to_i
      @number_batches_in_progress = xml_data['numberBatchesInProgress'].to_i
      @number_batches_completed = xml_data['numberBatchesCompleted'].to_i
      @number_batches_failed = xml_data['numberBatchesFailed'].to_i
      @total_batches = xml_data['totalBatches'].to_i
      @number_records_processed = xml_data['numberRecordsProcessed'].to_i
      @number_records_failed = xml_data['numberRecordsFailed'].to_i
      @total_processing_time = xml_data['totalProcessingTime'].to_i
      @api_active_processing_time = xml_data['apiActiveProcessingTime'].to_i
      @apex_processing_time = xml_data['apexProcessingTime'].to_i
    end

    def batch_list
      @client.get_batch_list(@id)
    end
    
    def create_batch data
      @client.create_batch(data)
    end

    def add_batch data
      @client.add_batch(data)
    end

    def close
      update(@client.close_job(@id))
    end

    def abort
      update(@client.abort_job(@id))
    end

    def refresh
      update(@client.get_job_info(@id))
    end

    def get_results
      batch_list.map(&:result).flatten
    end

    #Statuses
    def batches_finished?
      (@number_batches_queued == 0) and
      (@number_batches_in_progress == 0)
    end
    
    def finished?
      failed?  or 
      aborted? or 
      (closed? and batches_finished?)
    end

    def failed?
      state? 'Failed'
    end

    def aborted?
      state? 'Aborted'
    end
    
    def closed?
      state? 'Closed'
    end
    
    def open?
      state? 'Open'
    end
    
    def state?(value)
      @state.present? && @state.casecmp(value) == 0
    end
  end
end
