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
      def construct(pig_path, pigunit_path)
        @pig_path = pig_path if @pig_path.nil?
        @pigunit_path = pigunit_path if @pigunit_path.nil?

        fail ArgumentError, 'This version pigspec only can a unique pig_path for one process.' unless @pig_path == pig_path
        fail ArgumentError, 'This version pigspec only can a unique pigunit_path for one process.' unless @pigunit_path == pigunit_path

        @bridge ||= JavaBridge.new @pig_path, @pigunit_path
      end
    end

    def setup(pig_path, pigunit_path)
      Test.construct pig_path, pigunit_path
      @script = []
      @args = []
      @override = []
      @pickup = nil
      # rescue ArgumentError
      # raise 'Must needs pig_path and pigunit_path. It must point to your installation of Apache Pig/PigUnit jar files.'
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
        query  = "#{item[:name]} = LOAD '#{temp}' USING PigStorage('\\t')"
        query += " AS #{schema}" unless schema.nil?
        query += ';'
        test.override item[:name], query
      end
    end
  end # class Test

module_function

  def pig(
    pig_path = File.join(ENV['PIG_HOME'], 'pig.jar'),
    pigunit_path = File.join(ENV['PIG_HOME'], 'pigunit.jar'),
    &block
  )
    test = Test.new
    test.setup(pig_path, pigunit_path)
    test.evaluate(&block)
    result = test.run
    test.shutdown
    result
  end
end # module PigSpec
