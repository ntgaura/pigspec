$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
RSpec.configure do |c|
  c.filter_run_excluding skip: true
end
require 'pigspec'
