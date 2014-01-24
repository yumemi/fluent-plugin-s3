require 'fluent/plugin/s3send'
require 'fluent/log'

describe S3AltOutputModule::S3Send do
  context :add do
    require 'zlib'
    before do
      @buffer_dir = File.expand_path('../__buffer_dir__', __FILE__)
      FileUtils.rmtree(@buffer_dir)
      @obj = S3AltOutputModule::S3Send.new({}, {:buffer_dir => @buffer_dir, :s3_bucket => 'mybucket', :verify_file => true})
      @src_path = File.join(@buffer_dir, 'src_path')
      File.open(@src_path, 'w') do |tmp|
        w = Zlib::GzipWriter.new(tmp)
        w.write('x' * 100)
        w.close
      end
      $log ||= Fluent::Log.new
    end

    after do
      FileUtils.rmtree(@buffer_dir)
    end

    it 'should call :verify_file_format' do
      FileUtils.stub(:copy)
      expect(@obj).to receive(:verify_file_format)
      @obj.add(nil, nil)
    end

    it 'should not call :verify_file_format if not :verify_file' do
      @obj = S3AltOutputModule::S3Send.new({}, {:buffer_dir => @buffer_dir, :s3_bucket => 'mybucket', :verify_file => false})
      FileUtils.stub(:copy)
      expect(@obj).not_to receive(:verify_file_format)
      @obj.add(nil, nil)
    end

    it 'should return true if success' do
      expect(@obj.add(@src_path, 's3path')).to be true
    end

    it 'should raise if meta_file is not json format' do
      File.stub(:write) {|path, data|
        File.open(path, 'w') do |f|
          f.write(data)
          f.write('error')
        end
      }
      expect{@obj.add(@src_path, 's3path')}.to raise_error
    end

    it 'should raise if gzip_file is broken' do
      File.open(@src_path, 'w+') {|f| f.write('gomi')}
      expect{@obj.add(@src_path, 's3path')}.to raise_error
    end

    it 'should output logs before raise by meta_file error' do
      File.stub(:write) {|path, data|
        File.open(path, 'w') do |f|
          f.write(data)
          f.write('error')
        end
      }
      expect($log).to receive(:warn)
      expect{@obj.add(@src_path, 's3path')}.to raise_error
    end

    it 'should output logs before raise by gzip file error' do
      File.open(@src_path, 'w+') {|f| f.write('gomi')}
      expect($log).to receive(:warn)
      expect{@obj.add(@src_path, 's3path')}.to raise_error
    end
  end
end
