require 'rjb'
require 'tempfile'

# A testing framework for pig
module PigSpec
  # bridge for java pig classies
  class JavaBridge
    def initialize(pig_home = ENV['PIG_HOME'])
      fail ArgumentError, 'pig_home must not be nil.' unless pig_home

      Rjb.add_classpath(File.join(pig_home, 'pig.jar'))
      Rjb.add_classpath(File.join(pig_home, 'pigunit.jar'))

      Rjb.load '.', ['-Dfile.encoding=UTF-8']

      Rjb.add_jar(File.join(pig_home, 'pig.jar'))
      Rjb.add_jar(File.join(pig_home, 'pigunit.jar'))

      import_classies
    end

    def unload
      @pig_test_class = nil
      @cluster_class = nil
      @file_localizer_class = nil
      @pig_server_class = nil
      @schema_class = nil
      @string_builder_class = nil
      @data_type_enum = nil
      @string_util_class = nil
      Rjb.unload
      GC.start
    end

    def create_test(script, *args)
      instance = @pig_test_class.new script, args
      JavaPigTest.new self, instance
    end

    def context
      @pig_test_class.getPigServer.getPigContext
    end

    def create_hdfs_temp
      @file_localizer_class.getTemporaryPath(context).toString
    end

    def server
      @pig_test_class.getPigServer
    end

    def cluster
      @pig_test_class.getCluster
    end

    def upload_text(text, path)
      cluster.copyFromLocalFile(text, path, true)
    end

    def schema(alias_name)
      raw_schema = server.dumpSchema(alias_name)
      builder = @string_builder_class.new
      @schema_class.stringifySchema(builder, raw_schema, @data_type_enum.TUPLE)
      builder.toString
    end

    def stringify(alias_values)
      @string_util_class.join(alias_values, "\n")
    end

  private

    def import_classies
      @pig_test_class = Rjb.import('org.apache.pig.pigunit.PigTest')
      @cluster_class = Rjb.import('org.apache.pig.pigunit.Cluster')
      @file_localizer_class = Rjb.import('org.apache.pig.impl.io.FileLocalizer')
      @pig_server_class = Rjb.import('org.apache.pig.pigunit.pig.PigServer')
      @schema_class = Rjb.import('org.apache.pig.impl.logicalLayer.schema.Schema')
      @string_builder_class = Rjb.import('java.lang.StringBuilder')
      @data_type_enum = Rjb.import('org.apache.pig.data.DataType')
      @string_util_class = Rjb.import('org.apache.commons.lang.StringUtils')
    end
  end

  # Wrapper for java Pigtest class.
  class JavaPigTest
    def initialize(bridge, instance)
      @bridge = bridge
      @instance = instance
    end

    def register_script
      # runScript method only register pigscript.
      # pig uses 'lazy run' to decide really output alias.
      @instance.runScript
    end

    def run_script(goal_alias)
      # TODO: NOT stringify, but read pig types and cast ruby type.
      @bridge.stringify(@instance.getAlias(goal_alias)).split("\n")
    end

    def override(name, query)
      @instance.override name, query
    end
  end
end # module PigSpec
