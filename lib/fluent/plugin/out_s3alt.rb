module Fluent

  require 'fluent/mixin/config_placeholders'
  require 'fluent/plugin/s3send'

  class S3AltOutput < Fluent::TimeSlicedOutput
    Fluent::Plugin.register_output('s3alt', self)

    def initialize
      super
      require 'aws-sdk'
      require 'zlib'
      require 'time'
      require 'tempfile'
      require 'securerandom'

      @use_ssl = true
    end

    config_param :path, :string, :default => ""
    config_param :time_format, :string, :default => nil

    include SetTagKeyMixin
    config_set_default :include_tag_key, false

    include SetTimeKeyMixin
    config_set_default :include_time_key, false

    #config_set_default :buffer_chunk_limit, 256*1024*1024  # overwrite default buffer_chunk_limit

    config_param :aws_key_id, :string, :default => nil
    config_param :aws_sec_key, :string, :default => nil
    config_param :s3_bucket, :string
    config_param :s3_endpoint, :string, :default => nil
    config_param :s3_object_key_format, :string, :default => "%{path}%{time_slice}_%{index}.%{file_extension}"
    config_param :auto_create_bucket, :bool, :default => true
    #config_set_default :flush_interval, nil
    config_param :time_slice_wait, :time, :default => 10*60
    config_param :time_format2, :string, :default => "%Y%m%d%H%M"

    ## S3Send Config
    config_param :send_async, :bool, :default => false
    config_param :send_buffer_dir, :string
    config_param :send_max_thread, :integer, :default => 5
    ##

    attr_reader :bucket

    include Fluent::Mixin::ConfigPlaceholders

    def placeholders
      [:percent]
    end

    def configure(conf)
      super

      if format_json = conf['format_json']
        @format_json = true
      else
        @format_json = false
      end

      if use_ssl = conf['use_ssl']
        if use_ssl.empty?
          @use_ssl = true
        else
          @use_ssl = Config.bool_value(use_ssl)
          if @use_ssl.nil?
            raise ConfigError, "'true' or 'false' is required for use_ssl option on s3 output"
          end
        end
      end
      if @send_async
        raise 'send_buffer_dir must be specified when send_async == true' unless @send_buffer_dir
      end

      @pid = $$

      @timef = TimeFormatter.new(@time_format, @localtime)
      @timef2 = TimeFormatter.new(@time_format2, @localtime)
    end

    def start
      super
      options = {}
      if @aws_key_id && @aws_sec_key
        options[:access_key_id] = @aws_key_id
        options[:secret_access_key] = @aws_sec_key
      end
      options[:s3_endpoint] = @s3_endpoint if @s3_endpoint
      options[:use_ssl] = @use_ssl

      @s3 = AWS::S3.new(options)
      @bucket = @s3.buckets[@s3_bucket]
      @s3_options = options

      ensure_bucket
      check_apikeys
      if @send_async
        s3send.run
      end
    end

    def s3send
      @s3send ||= S3AltOutputModule::S3Send.new(@s3_options,
                                                :buffer_dir => @send_buffer_dir,
                                                :s3_bucket => @s3_bucket,
                                                :max_conn => @send_max_thread)
    end

    def format(tag, time, record)
      if @include_time_key || !@format_json
        time_str = @timef.format(time)
      end

      # copied from each mixin because current TimeSlicedOutput can't support mixins.
      if @include_tag_key
        record[@tag_key] = tag
      end
      if @include_time_key
        record[@time_key] = time
      end
      #record['hogehoge_at'] = time
      #record['time_key'] = @time_key

      if @format_json
        Yajl.dump(record) + "\n"
      else
        "#{time_str}\t#{tag}\t#{Yajl.dump(record)}\n"
      end
    end

    def write(chunk)
      i = 0
      time2 = @timef2.format(Engine.now.to_i)
      begin
        values_for_s3_object_key = {
            "path" => @path,
            "time_slice" => chunk.key,
            "file_extension" => "gz",
            "index" => i,
            "index0" => sprintf("%04d", i),
            "pid" => @pid,
            "time2" => time2,
            "this_uuid" => SecureRandom.uuid
        }
        s3path = @s3_object_key_format.gsub(%r(%{[^}]+})) { |expr|
          values_for_s3_object_key[expr[2...expr.size-1]]
        }
        i += 1
      end while @bucket.objects[s3path].exists?
      tmp = Tempfile.new("s3alt-")
      w = Zlib::GzipWriter.new(tmp)
      begin
        chunk.write_to(w)
        w.close
        if @send_async then
          s3send.add(tmp.path, s3path, :content_type => 'application/x-gzip')
        else
          @bucket.objects[s3path].write(Pathname.new(tmp.path), :content_type => 'application/x-gzip')
        end
      ensure
        tmp.close(true) rescue nil
        w.close rescue nil
      end
    end

    private

    def ensure_bucket
      if !@bucket.exists?
        if @auto_create_bucket
          $log.info "Creating bucket #{@s3_bucket} on #{@s3_endpoint}"
          @s3.buckets.create(@s3_bucket)
        else
          raise "The specified bucket does not exist: bucket = #{@s3_bucket}"
        end
      end
    end

    def check_apikeys
      @bucket.empty?
    rescue
      raise "aws_key_id or aws_sec_key is invalid. Please check your configuration"
    end
  end
end

