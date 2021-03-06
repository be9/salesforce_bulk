# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "salesforce_bulk2/version"

Gem::Specification.new do |s|
  s.name        = "salesforce_bulk2"
  s.version     = SalesforceBulk2::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Adam Kerr", "Jorge Valdivia", "Javier Julio"]
  s.email       = ["ajrkerr@gmail.com", "jorge@valdivia.me", "jjfutbol@gmail.com"]
  s.homepage    = "https://github.com/ajrkerr/salesforce_bulk"
  s.summary     = %q{Ruby support for the Salesforce Bulk API}
  s.description = %q{This gem is a simple interface to the Salesforce Bulk API providing support for insert, update, upsert, delete, and query.}

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  s.add_dependency "activesupport"
  s.add_dependency "xml-simple"

  s.add_development_dependency "mocha"
  s.add_development_dependency "rake"
  s.add_development_dependency "shoulda"
  s.add_development_dependency "webmock"
end
