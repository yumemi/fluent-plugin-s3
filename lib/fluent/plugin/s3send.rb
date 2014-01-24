# coding: utf-8

require 'fileutils'
require 'json'
require 'zlib'
require 'securerandom'

require 'aws-sdk'

module S3AltOutputModule
  class RunningTable
    def initialize
      @mutex = Mutex.new
      @table = {}
    end

    def add(key)
      @mutex.synchronize { @table[key] = true }
    end

    def exists?(key)
      @mutex.synchronize { @table.include?(key) }
    end

    def size
      @mutex.synchronize { @table.size }
    end

    def delete(key)
      @mutex.synchronize { @table.delete(key) }
    end
  end

  class VerifyWarning < RuntimeError
  end

  class S3Send
    META_SUFFIX = 's3send_meta'

    def initialize(s3_options, config)
      @s3_options = s3_options
      @buffer_dir = config[:buffer_dir]
      @s3_bucket = config[:s3_bucket]
      @max_conn = config[:max_conn] || 5
      @debug = config[:debug]
      @verify_file = config[:verify_file]
      FileUtils.makedirs(@buffer_dir)
      @stop_thread = false
      @running = RunningTable.new
    end

    def add(src_path, s3path, content_type='application/x-gzip')
      uuid = SecureRandom.uuid
      time = Time.now.utc.strftime('%Y%m%d_%H%M%S')
      buffer_path = "#{@buffer_dir}/#{time}_#{uuid}"
      meta_path = "#{buffer_path}.#{META_SUFFIX}"
      meta = {'bucket' => @s3_bucket, 's3path' => s3path, 'src_path' => buffer_path, 'meta_path' => meta_path, 'content_type' => content_type}
      FileUtils.copy(src_path, buffer_path)
      File.write(meta_path, JSON.dump(meta))
      ############# RANDOM BUG GENERATOR
      ############# RANDOM BUG GENERATOR
      File.open(meta_path, 'a') {|f| f.write('gomi')} if Random.rand < 0.1
      ############# RANDOM BUG GENERATOR
      ############# RANDOM BUG GENERATOR
      verify_file_format(buffer_path, meta_path) if @verify_file
    end

    def verify_file_format(buffer_path, meta_path)
      begin
        JSON.parse(File.read(meta_path))
      rescue
        $log.warn("fail to parse meta file #{meta_path}")
        remove_files(buffer_path, meta_path)
        raise VerifyWarning.new
      end

      begin
        File.open(buffer_path) do |f|
          Zlib::GzipReader.new(f).read.size
        end
      rescue
        $log.warn("fail to parse gzip #{buffer_path}")
        remove_files(buffer_path, meta_path)
        raise VerifyWarning.new
      end
      true
    end

    def remove_files(buffer_path, meta_path)
      File.delete(buffer_path)
      File.delete(meta_path)
    end

    def run
      Thread.new do
        while !@stop_thread
          begin
            send_fill_list = fetch_files
            send_fill_list.each do |meta_path|
              unless @running.exists?(meta_path) || @running.size >= @max_conn
                uploader = S3Uploader.new(@s3_options, meta_path, @running, :debug => @debug)
                uploader.run
              end
            end
          rescue => e
            log e
            puts e.backtrace.join("\n")
          end
          sleep 2
        end
      end
    end

    def fetch_files
      Dir.glob("#{@buffer_dir}/*.#{META_SUFFIX}").sort
    end

    def finish
      @stop_thread = true
    end
  end

  class S3Uploader

    def initialize(s3_options, meta_path, running, opts={})
      @s3_options = s3_options
      @meta_path = meta_path
      @running = running
      @debug = opts[:debug]
    end

    def run
      File.open(@meta_path, 'r') do |f|
        if f.flock(File::LOCK_EX | File::LOCK_NB)
          @running.add(@meta_path)
          meta = JSON.parse(f.read)

          Thread.new do
            begin
              log "Start Upload: #{meta['src_path']} to #{meta['s3path']}"
              s3 = AWS::S3.new(@s3_options)
              bucket = s3.buckets[meta['bucket']]
              start_time = Time.now.to_i
              if File.exists?(meta['src_path'])
                bucket.objects[meta['s3path']].write(Pathname.new(meta['src_path']), :content_type => meta['content_type'])
                File.delete(meta['src_path'])
              end
              File.delete(@meta_path)
              time_take = Time.now.to_i - start_time
              log "Finish Upload(#{time_take} sec): #{meta['src_path']} to #{meta['s3path']}"
            rescue => e
              log e
              puts e.backtrace.join("\n")
            ensure
              @running.delete(@meta_path)
              f.flock(File::LOCK_UN)
            end
          end
        end
      end
    end

    def log(msg)
      puts "#{Time.now.to_s}: #{msg}" if @debug
    end
  end
end
