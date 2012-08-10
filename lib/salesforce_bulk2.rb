require 'net/https'
require 'xmlsimple'
require 'csv'
require 'active_support'
require 'active_support/inflector'
require 'active_support/core_ext/object/blank'
require 'active_support/core_ext/hash/keys'
require 'salesforce_bulk2/version'
require 'salesforce_bulk2/core_extensions/string'
require 'salesforce_bulk2/salesforce_error'
require 'salesforce_bulk2/client'
require 'salesforce_bulk2/job'
require 'salesforce_bulk2/batch'
require 'salesforce_bulk2/batch_result'
require 'salesforce_bulk2/batch_result_collection'
require 'salesforce_bulk2/query_result_collection'

module SalesforceBulk2
end