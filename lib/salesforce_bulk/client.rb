module SalesforceBulk
  # Interface for operating the Salesforce Bulk REST API
  class Client
    # The HTTP connection we will be using to connect to Salesforce.com
    attr_accessor :connection

    def initialize(options={})
      @connection = Connection.new(options)
    end

    def connected?
      @connection.connected?
    end

    def disconnect
      @connection.disconnect
    end

    def connect options = {}
      @connection.connect(options)
    end

    def new_job operation, sobject, options = {}
      Job.new(add_job(operation, sobject, options), self)
    end

    def add_job operation, sobject, options={}
      operation = operation.to_sym.downcase
      
      raise ArgumentError.new("Invalid operation: #{operation}") unless Job.valid_operation?(operation)
      
      options.assert_valid_keys(:external_id_field_name, :concurrency_mode)
      
      if options[:concurrency_mode]
        concurrency_mode = options[:concurrency_mode].capitalize
        raise ArgumentError.new("Invalid concurrency mode: #{concurrency_mode}") unless Job.valid_concurrency_mode?(concurrency_mode)
      end
      
      xml  = '<?xml version="1.0" encoding="utf-8"?>'
      xml += '<jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">'
      xml += "  <operation>#{operation}</operation>"
      xml += "  <object>#{sobject}</object>" if sobject
      xml += "  <externalIdFieldName>#{options[:external_id_field_name]}</externalIdFieldName>" if options[:external_id_field_name]
      xml += "  <concurrencyMode>#{options[:concurrency_mode]}</concurrencyMode>" if options[:concurrency_mode]
      xml += "  <contentType>CSV</contentType>"
      xml += "</jobInfo>"
      
      @connection.http_post_xml("job", xml)
    end

    def abort_job job_id
      xml  = '<?xml version="1.0" encoding="utf-8"?>'
      xml += '<jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">'
      xml += '  <state>Aborted</state>'
      xml += '</jobInfo>'
      
      @connection.http_post_xml("job/#{job_id}", xml)
    end

    def close_job job_id
      xml  = '<?xml version="1.0" encoding="utf-8"?>'
      xml += '<jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">'
      xml += '  <state>Closed</state>'
      xml += '</jobInfo>'
      
      @connection.http_post_xml("job/#{job_id}", xml)
    end

    def get_job_info job_id
      @connection.http_get_xml("job/#{job_id}")
    end

    def get_batch_info job_id, batch_id
      @connection.http_get_xml("job/#{jobId}/batch/#{batchId}")
    end

    def find_job job_id
      Job.new get_job(job_id)
    end

    def find_batch job_id, batch_id
      Batch.new get_batch(job_id, batch_id)
    end

    def create_batch job_id, data
      Batch.new add_batch(job_id, data)
    end

    def add_batch job_id, data
      body = data
      
      if data.is_a?(Array)
        raise ArgumentError, "Batch data set exceeds #{Batch.max_records} record limit by #{data.length - Batch.max_records}" if data.length > Batch.max_records
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
    
    def get_batch_list(job_id)
      result = @connection.http_get_xml("job/#{job_id}/batch")
      
      if result['batchInfo'].is_a?(Array)
        result['batchInfo'].collect { |info| Batch.new(info) }
      else
        [Batch.new(result['batchInfo'])]
      end
    end

    def get_batch_request(job_id, batch_id)
      response = http_get("job/#{job_id}/batch/#{batch_id}/request")
        
      CSV.parse(response.body, :headers => true) do |row|
        result << BatchResult.new(row[0], row[1].to_b, row[2].to_b, row[3])
      end
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

    ## Operations
    def delete(sobject, data)
      perform_operation(:delete, sobject, data)
    end
    
    def insert(sobject, data)
      perform_operation(:insert, sobject, data)
    end
    
    def query(sobject, data)
      perform_operation(:query, sobject, data)
    end
    
    def update(sobject, data)
      perform_operation(:update, sobject, data)
    end
    
    def upsert(sobject, external_id, data)
      perform_operation(:upsert, sobject, data, external_id)
    end
    
    def perform_operation(operation, sobject, data, external_id = nil, batch_size = nil)
      job = new_job(operation, sobject, :external_id_field_name => external_id)
      
      data.each_slice(batch_size || Batch.batch_size) do |records|
        job.add_batch(records)
      end

      job.close
      
      until job.finished?
        job.refresh
        sleep 2
      end
      
      job.get_results
    end
  end
end