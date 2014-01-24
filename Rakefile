
require 'bundler'
Bundler::GemHelper.install_tasks

require 'rake/testtask'
require 'rspec/core/rake_task'

Rake::TestTask.new(:test) do |test|
  test.libs << 'lib' << 'test'
  test.test_files = FileList['test/*.rb']
  test.verbose = true
end

RSpec::Core::RakeTask.new(:spec)

task :default => [:build]

