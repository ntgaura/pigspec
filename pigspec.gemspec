# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'pigspec/version'

Gem::Specification.new do |spec|
  spec.name          = 'pigspec'
  spec.version       = PigSpec::VERSION
  spec.authors       = ['shiracha']
  spec.email         = ['shiracha.rikyu@gmail.com']
  spec.summary       = 'A Testing Framework extension for Apache Pig.'
  spec.description   = 'A Testing Framework extension for Apache Pig.To setup, executing and get the result.'
  spec.homepage      = ''
  spec.license       = 'MIT'

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(/^bin\//) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(/^(test|spec|features)\//)
  spec.require_paths = ['lib']

  spec.add_dependency 'rjb'
  spec.add_dependency 'cleanroom'

  spec.add_development_dependency 'bundler', '~> 1.7'
  spec.add_development_dependency 'rake', '~> 10.0'
  spec.add_development_dependency 'rspec'
  spec.add_development_dependency 'rubocop'
end
