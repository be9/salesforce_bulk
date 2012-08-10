module SalesforceBulk2
  class BatchResultCollection < Array
    
    def any_failures?
      self.any? { |result| result.error? }
    end
    
    def failed
      self.select { |result| result.error? }
    end
    
    def completed
      self.select { |result| result.successful? }
    end
    
    def created
      self.select { |result| result.successful? && result.created? }
    end
  end
end