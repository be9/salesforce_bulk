module SalesforceBulk2
  class BatchResult < Hash
    def initialize(id, success, created, error)
      self['id'] = id
      self['success'] = success
      self['created'] = created
      self['error'] = error
    end
    
    def error?
      error.present?
    end
    
    def created?
      created
    end
    
    def successful?
      success
    end
    
    def updated?
      !created && success
    end

    def method_missing method, *args, &block
      if has_key? method.to_s
        self[method.to_s] 
      else
        super method, *args, &block
      end
    end

    def respond_to? method
      if has_key? method.to_sym
        return true
      else
        super
      end
    end
  end
end