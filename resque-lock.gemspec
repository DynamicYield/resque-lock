Gem::Specification.new do |s|
  s.name        = 'resque-lock'
  s.version     = '0.0.1'
  s.date        = '2016-01-13'
  s.summary     = 'Limited number of parallel Resque job runs'
  s.description = 'Limited number of parallel Resque job runs'
  s.authors     = ['Dynamic Yield']
  s.email       = 'support@dynamicyield.com'
  s.files       = ['LICENSE', 'Gemfile', "#{s.name}.gemspec"] + Dir['lib/*.rb'] + Dir['lib/**/*.rb']
  s.homepage    = ''
  s.license     = 'MIT'

  s.add_dependency 'resque', '~> 1.20'
  s.add_dependency 'resque-reserve-hooks', '~> 0.0.2'
end
