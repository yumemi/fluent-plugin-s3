#coding: utf-8

require 'rubygems'
require_relative '../lib/fluent/plugin/s3send'

DATA_DIR = 'data'

max_conn = ARGV.shift.to_s.to_i
max_conn = max_conn == 0 ? 5 : max_conn
puts "max_conn=#{max_conn}"

s3_options = {
    :access_key_id => ENV['AWS_ACCESS_KEY_ID'],
    :secret_access_key => ENV['AWS_SECRET_ACCESS_KEY'],
    :use_ssl => true
}

config = {
    :s3_bucket => 'tsredshift',
    :buffer_dir => '/tmp/buffer_dir',
    :max_conn => max_conn,
    :debug => true,
}

s3sync = S3AltOutputModule::S3Send.new(s3_options, config)
s3sync.run

while true
  Dir.glob("#{DATA_DIR}/**/*") do |filename|
    unless File.directory?(filename)
      puts "FIND: #{filename}"
      s3sync.add(filename, "s3sync/#{filename}")
      File.delete(filename)
    end
  end
  sleep 5
end

