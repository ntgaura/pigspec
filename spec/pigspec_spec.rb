require 'spec_helper'

# ---------------------- test data constants
input_alias = 'in'
input = %w(
  hoge
  hoge
  hoge
  hoge
  haga
  haga
  hage
  hage
  hage
  huge
)
output_alias = 'out'
output = %w((hoge))

# ---------------------- test

include PigSpec

describe PigSpec do
  it 'has a version number' do
    expect(PigSpec::VERSION).not_to be nil
  end

  it 'can run' do
    actual = pig do
      script <<-EOS
        in = LOAD 'inputfile' AS (query:chararray);
        out = LIMIT in 1;
        STORE out INTO 'outputfile';
      EOS
      with_args 'n=2'
      override input_alias, input
      pickup output_alias
    end
    expect(actual).to eq(output)
  end

  it 'can run from file' do
    actual = pig do
      script_file 'spec/pig/test.pig'
      with_args 'n=2'
      override input_alias, input
      pickup output_alias
    end
    expect(actual).to eq(output)
  end

  it 'can import pig macro(script)' do
    actual = pig do
      script <<-EOS
        IMPORT 'spec/pig/test_macro.pig';
        in = LOAD 'inputfile' AS (query:chararray);
        macro_in = LIMIT in 1;
        out = test_macro(macro_in, 'query');
        STORE out INTO 'outputfile';
      EOS
      with_args 'n=2'
      override input_alias, input
      pickup output_alias
    end
    expect(actual).to eq(%w((testconcat_hoge)))
  end

  it 'can import pig macro(script_file)' do
    actual = pig do
      script_file 'spec/pig/test2.pig'
      with_args 'n=2'
      override input_alias, input
      pickup output_alias
    end
    expect(actual).to eq(%w((testconcat_hoge)))
  end

  # TODO: avoiding duplicated macro error...
  it 'can define macro on script', skip: true  do
    actual = pig do
      script <<-EOS
        DEFINE counting_macro(rel, column) RETURNS RET {
            rel_00 = GROUP $rel BY ($column);
            $RET = FOREACH rel_00 GENERATE group as word, COUNT($rel) AS count;
        };
        in_00 = LOAD 'inputfile' AS (query:chararray);
        out_00 = counting_macro(in_00, 'query');
        out = ORDER out_00 BY count DESC;
        STORE out INTO 'outputfile';
      EOS
      with_args 'n=2'
      override 'in_00', input
      pickup output_alias
    end
    expect(actual).to eq(%w((hoge,4) (hage,3) (haga,2) (huge,1)))
  end

  it 'can override macro relation' do
    actual = pig do
      script <<-EOS
        IMPORT 'spec/pig/counting_macro.pig';
        in_00 = LOAD 'inputfile' AS (query:chararray);
        out_00 = counting_macro(in_00, 'query');
        out = ORDER out_00 BY count DESC;
        STORE out INTO 'outputfile';
      EOS
      with_args 'n=2'
      override 'in_00', input
      pickup output_alias
    end
    expect(actual).to eq(%w((hoge,4) (hage,3) (haga,2) (huge,1)))
  end
end
