module SalesforceBulk2
  class Job
    attr_reader :client

    attr_reader :concurrency_mode
    attr_reader :external_id
    attr_reader :data
    attr_reader :xml_data

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

    def self.create client, options = {}
      job = Job.new(client)

      options.assert_valid_keys(:external_id, :concurrency_mode, :object, :operation)

      operation = options[:operation].to_sym.downcase
      raise ArgumentError.new("Invalid operation: #{operation}") unless Job.valid_operation?(operation)

      external_id = options[:external_id]
      concurrency_mode = options[:concurrency_mode]
      object = options[:object]

      if concurrency_mode
        concurrency_mode = concurrency_mode.capitalize
        raise ArgumentError.new("Invalid concurrency mode: #{concurrency_mode}") unless Job.valid_concurrency_mode?(concurrency_mode)
      end

      xml  = '<?xml version="1.0" encoding="utf-8"?>'
      xml += '<jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">'
      xml += "  <operation>#{operation}</operation>"
      xml += "  <object>#{object}</object>" if object
      xml += "  <externalIdFieldName>#{external_id}</externalIdFieldName>" if external_id
      xml += "  <concurrencyMode>#{concurrency_mode}</concurrencyMode>" if concurrency_mode
      xml += "  <contentType>CSV</contentType>"
      xml += "</jobInfo>"

      job.update(client.http_post_xml("job", xml))
      job
    end

    def self.find client, id
      job = Job.new(client)
      job.refresh(id)
      job
    end

    def refresh(id = nil)
      @id ||= id
      xml_data = @client.http_get_xml("job/#{@id}")
      update(xml_data)
    end

    def initialize client
      @client = client
    end

    def new_batch data
      Batch.create(self, data)
    end

    def get_batches
      result = @client.http_get_xml("job/#{@id}/batch")

      if result['batchInfo'].is_a?(Array)
        result['batchInfo'].collect { |info| Batch.new(self, info) }
      elsif result['batchInfo']
        [Batch.new(self, result['batchInfo'])]
      else
        []
      end
    end

    def abort
      xml  = '<?xml version="1.0" encoding="utf-8"?>'
      xml += '<jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">'
      xml += '  <state>Aborted</state>'
      xml += '</jobInfo>'

      @client.http_post_xml("job/#{@id}", xml)
    end

    def close
      xml  = '<?xml version="1.0" encoding="utf-8"?>'
      xml += '<jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">'
      xml += '  <state>Closed</state>'
      xml += '</jobInfo>'

      @client.http_post_xml("job/#{@id}", xml)
    end

    def update xml_data
      #Assign object
      @xml_data = xml_data

      #Mass assign the defaults
      @@fields.each do |field|
        instance_variable_set(:"@#{field}", xml_data[field.to_s])
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

    def add_data data, batch_size = nil
      data.each_slice(batch_size || Batch.batch_size) do |records|
        new_batch(records)
      end
    end

    def get_results
      results = BatchResultCollection.new

      get_batches.each { |batch| results << batch.get_result }

      results.flatten
    end

    # def get_requests
    #   results = BatchResultCollection.new

    #   get_batches.each { |batch| results << batch.get_request }

    #   results.flatten
    # end

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
