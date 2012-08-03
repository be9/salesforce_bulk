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


    def initialize job
      @job = job
      @connection = job.connection
    end

    def self.create job, data
      batch = Batch.new(job)
      batch.execute(data)
    end

    def self.find job, batch_id
      batch = Batch.new job
      batch.id = batch_id
      batch.refresh
      batch
    end

    def self.find job, batch_id
      @job = job
      @connection = job.connection
    end

    def execute data
      @data = data
      body = data
      
      if data.is_a?(Array)
        raise ArgumentError, "Batch data set exceeds #{@@batch_size} record limit by #{data.length - @@batch_size}" if data.length > @@batch_size
        raise ArgumentError, "Batch data set is empty" if data.length < 1
        
        keys = data.first.keys
        body = keys.to_csv
        
        data.each do |item|
          item_values = keys.map { |key| item[key] }
          body += item_values.to_csv
        end
      end
      
      # Despite the content for a query operation batch being plain text we 
      # still have to specify CSV content type per API docs.
      @connection.http_post_xml("job/#{job_id}/batch", body, "Content-Type" => "text/csv; charset=UTF-8")
    end

    def refresh
      @connection.http_get_xml("job/#{@job_id}/batch/#{@batch_id}")
    end

    def get_batch_request(job_id, batch_id)
      response = http_get("job/#{job_id}/batch/#{batch_id}/request")

      CSV.parse(response.body, :headers => true)
    end
    
    def get_batch_result(job_id, batch_id)
      response = http_get("job/#{job_id}/batch/#{batch_id}/result")
      
      #Query Result
      if response.body =~ /<.*?>/m
        result = XmlSimple.xml_in(response.body)
        
        if result['result'].present?
          results = get_query_result(job_id, batch_id, result['result'].first)
          
          collection = QueryResultCollection.new(self, job_id, batch_id, result['result'].first, result['result'])
          collection.replace(results)
        end

      #Batch Result
      else
        result = BatchResultCollection.new(job_id, batch_id)
        
        CSV.parse(response.body, :headers => true) do |row|
          result << BatchResult.new(row[0], row[1].to_b, row[2].to_b, row[3])
        end
        
        result
      end
    end
    
    def get_query_result(job_id, batch_id, result_id)
      headers = {"Content-Type" => "text/csv; charset=UTF-8"}
      response = http_get("job/#{job_id}/batch/#{batch_id}/result/#{result_id}", headers)
      
      lines = response.body.lines.to_a
      headers = CSV.parse_line(lines.shift).collect { |header| header.to_sym }
      
      result = []
      
      #CSV.parse(lines.join, :headers => headers, :converters => [:all, lambda{|s| s.to_b if s.kind_of? String }]) do |row|
      CSV.parse(lines.join, :headers => headers) do |row|
        result << Hash[row.headers.zip(row.fields)]
      end
      
      result
    end

    def self.batch_size
      @@batch_size
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
      xml_data = @connection.http_get_xml("job/#{@job_id}/batch/#{@batch_id}")
      update(xml_data)
    end
  end
end