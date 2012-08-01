module SalesforceBulk
  class Connection
    # If true, print API debugging information to stdout. Defaults to false.
    attr_accessor :debugging

    # The host to use for authentication. Defaults to login.salesforce.com.
    attr_reader :host
    
    # The instance host to use for API calls. Determined from login response.
    attr_reader :instance_host
    
    # The Salesforce password
    attr_reader :password
    
    # The Salesforce security token
    attr_reader :token
    
    # The Salesforce username
    attr_reader :username
    
    # The API version the client is using. Defaults to 24.0.
    attr_reader :version


    # Defaults
    @@host = 'login.salesforce.com'
    @@version = 24.0
    @@debugging = false
    @@api_path_prefix = "/services/async/"

    def initialize options
      if options.is_a?(String)
        options = YAML.load_file(options)
        options.symbolize_keys!
      end
      
      options.assert_valid_keys(:username, :password, :token, :debugging, :host, :version)
      
      @username = options[:username]
      @password = "#{options[:password]}#{options[:token]}"
      @token    = options[:token]
      @host     = options[:host] || @@host
      @version  = options[:version] || @@version
      @debugging = options[:debugging] || @@debugging
    end

    def connect options = {}
      raise Error.new("Already connected") if connected?

      @username = options[:username] || @username
      @password = options[:password] || @password
      @version  = options[:version] || @version

      xml  = '<?xml version="1.0" encoding="utf-8"?>'
      xml += '<env:Envelope xmlns:xsd="http://www.w3.org/2001/XMLSchema"'
      xml += ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
      xml += ' xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">'
      xml += '  <env:Body>'
      xml += '    <n1:login xmlns:n1="urn:partner.soap.sforce.com">'
      xml += "      <n1:username>#{@username}</n1:username>"
      xml += "      <n1:password>#{@password}</n1:password>"
      xml += "    </n1:login>"
      xml += "  </env:Body>"
      xml += "</env:Envelope>"
      
      data = http_post_xml("/services/Soap/u/#{@version}", xml, 'Content-Type' => 'text/xml', 'SOAPAction' => 'login')
      result = data['Body']['loginResponse']['result']
      
      @session_id = result['sessionId']
      @server_url = result['serverUrl']
      @instance_id = instance_id(@server_url)
      @instance_host = "#{@instance_id}.salesforce.com"
      
      @api_path_prefix = "#{@@api_path_prefix}/#{@version}/"

      result
    end

    def disconnect
      xml  = '<?xml version="1.0" encoding="utf-8"?>'
      xml += '<env:Envelope xmlns:xsd="http://www.w3.org/2001/XMLSchema"'
      xml += ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
      xml += ' xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">'
      xml += '  <env:Body>'
      xml += '    <n1:logout xmlns:n1="urn:partner.soap.sforce.com" />'
      xml += '  </env:Body>'
      xml += '</env:Envelope>'
      
      result = http_post_xml("/services/Soap/u/#{@version}", xml, 'Content-Type' => 'text/xml', 'SOAPAction' => 'logout')

      @session_id = nil
      @server_url = nil
      @instance_id = nil
      @instance_host = nil
      @api_path_prefix = nil

      result
    end

    def connected?
      !!@session_id
    end

    def http_post(path, body, headers={})
      headers = {'Content-Type' => 'application/xml'}.merge(headers)
      
      #Are we connected?
      if connected?
        headers['X-SFDC-Session'] = @session_id
        host = @instance_host
        path = "#{@api_path_prefix}#{path}"
      else
        host = @host
      end

      response = https_request(host).post(path, body, headers)
      
      if response.is_a?(Net::HTTPSuccess)
        response
      else
        raise SalesforceError.new(response)
      end
    end
    
    def http_get(path, headers={})
      path = "#{@api_path_prefix}#{path}"
      
      headers = {'Content-Type' => 'application/xml'}.merge(headers)
      
      headers['X-SFDC-Session'] = @session_id if @session_id
      
      response = https_request(@instance_host).get(path, headers)
      
      if response.is_a?(Net::HTTPSuccess)
        response
      else
        raise SalesforceError.new(response)
      end
    end

    def http_post_xml(path, body, headers = {})
      XmlSimple.xml_in(http_post(path, body, headers).body, :ForceArray => false)
    end

    def http_get_xml(path, headers = {})
      XmlSimple.xml_in(http_get(path, headers).body, :ForceArray => false)
    end
    
    def https_request(host)
      req = Net::HTTP.new(host, 443)
      req.use_ssl = true
      req.verify_mode = OpenSSL::SSL::VERIFY_NONE
      req
    end

  private
    def instance_id(url)
      url.match(/:\/\/([a-zA-Z0-9-]{2,}).salesforce/)[1]
    end
  end
end
