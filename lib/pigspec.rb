require 'pigspec/version'
require 'pigspec/javabridge'

require 'cleanroom'
require 'tempfile'

# A testing framework for pig
module PigSpec
  # TestCase of PigSpec
  class Test
    include Cleanroom

    class << self
      attr_reader :bridge
      def construct
        @bridge ||= JavaBridge.new
      rescue ArgumentError
        raise 'Environment variable PIG_HOME does not found or does not correct. It must point to your installation of Apache Pig.'
      end
    end

    def setup
      Test.construct
      @script = []
      @args = []
      @override = []
      @pickup = nil
    end

    def shutdown
      # Rjb unload -> load is not work.(current restriction.)
      # s.a https://github.com/arton/rjb/issues/34
      # If the JNI bug fixed then we can uncomment follow line.
      # @bridge.unload
    end

    def script(text)
      @script = text.split("\n")
    end
    expose :script

    def script_file(path)
      @script = open(path, 'r') { |f| f.each_line.to_a }
    end
    expose :script_file

    def with_args(*args)
      @args = args
    end
    expose :with_args

    def override(name, value)
      @override.push name: name, value: value
    end
    expose :override

    # to 'expose'
    def pickup(name)  # rubocop:disable Style/TrivialAccessors
      @pickup = name
    end
    expose :pickup

    def run
      # If no output alias, must result nil
      return nil unless @pickup

      test = Test.bridge.create_test @script, *@args
      test.register_script
      apply_override test
      test.run_script @pickup
    end

  private

    def apply_override(test)
      @override.each do |item|
        temp = Test.bridge.create_hdfs_temp
        Test.bridge.upload_text item[:value], temp

        schema = Test.bridge.schema item[:name]

        query = "#{item[:name]} = LOAD '#{temp}' USING PigStorage('\\t') AS #{schema};"
        test.override item[:name], query
      end
    end
  end # class Test

module_function

  def pig(&block)
    test = Test.new
    test.setup
    test.evaluate(&block)
    result = test.run
    test.shutdown
    result
  end
end # module PigSpec
