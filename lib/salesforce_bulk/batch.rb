module SalesforceBulk
  class Batch
    attr_accessor :session_id

    attr_reader :apex_processing_time
    attr_reader :api_active_processing_time
    attr_reader :completed_at
    attr_reader :created_at
    attr_reader :failed_records
    attr_reader :id
    attr_reader :job_id
    attr_reader :processed_records
    attr_reader :state
    attr_reader :total_processing_time
    attr_reader :data

    @@batch_size = 10000

    def self.batch_size
      @@batch_size
    end

    def self.new_from_xml xml_data, session_id = nil
      batch = Batch.new
      batch.update(data)
      batch.session_id = session_id
      batch
    end

    def self.find job_id, batch_id, session_id
      batch = Batch.new
      batch.id = batch_id
      batch.job_id = job_id
      batch.session_id = session_id
      batch.refresh
      batch
    end


    def update(data)
      @data = data

      @id = data['id']
      @job_id = data['jobId']
      @state = data['state']
      @created_at = DateTime.parse(data['createdDate'])
      @completed_at = DateTime.parse(data['systemModstamp'])
      @processed_records = data['numberRecordsProcessed'].to_i
      @failed_records = data['numberRecordsFailed'].to_i
      @total_processing_time = data['totalProcessingTime'].to_i
      @api_active_processing_time = data['apiActiveProcessingTime'].to_i
      @apex_processing_time = data['apex_processing_time'].to_i
    end

    def job
      @job ||= Job.find(@job_id, @session_id) if @session_id
    end

    ### State Information ###
    def in_progress?
      state? 'InProgress'
    end
    
    def queued?
      state? 'Queued'
    end
    
    def completed?
      state? 'Completed'
    end
    
    def failed?
      state? 'Failed'
    end
    
    def finished?
      completed? or finished?
    end

    def state?(value)
      self.state.present? && self.state.casecmp(value) == 0
    end

    def errors?
      @number_records_failed > 0
    end

    def result
      @client.get_batch_result(@job_id, @batch_id)
    end

    def request
      @client.get_batch_request(@job_id, @batch_id)
    end

    def refresh
      xml_data = @connection.http_get_xml("job/#{jobId}/batch/#{batchId}")
      update(xml_data)
    end
  end
end