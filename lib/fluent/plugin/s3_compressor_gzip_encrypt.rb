module Fluent
  class S3Output
    class GzipCommandCompressorEncryptor < Compressor
      S3Output.register_compressor('gzip_encrypt', self)

      config_param :command_parameter, :string, :default => ''
      config_param :pass_file, :string, :default => '/etc/fluentd/pass.file'

      def configure(conf)
        super
        check_command('gzip')
        check_command('openssl')
      end

      def ext
        'gze'.freeze
      end

      def content_type
        'application/data'.freeze
      end

      def compress(chunk, tmp)
        chunk_is_file = @buffer_type == 'file'
        path = if chunk_is_file
                 chunk.path
               else
                 w = Tempfile.new("chunk-gzip-tmp")
                 chunk.write_to(w)
                 w.close
                 w.path
               end

        res = system "gzip #{@command_parameter} -c #{path} | openssl enc -aes-256-cbc -pass file:#{@pass_file} -out #{tmp.path}"
        unless res
          log.warn "failed to execute gzip/openssl command. status = #{$?}"
        end
      ensure
        unless chunk_is_file
          w.close(true) rescue nil
        end
      end
    end
  end
end
