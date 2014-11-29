# PigSpec

PigSpec is a extention for rspec testing framework for Apache Pig.
Execution pig with rewrite some relation and get output relation.

## Installation
PigSpec uses Pig and PigUnit.
Thus, install Pig and PigUnit first.

Second. install gem of pigspec.

If you using bundle, You write Gemfile to

```ruby
gem 'pigspec'
```
And then execute:

```bash
`$ bundle`
```

Or install it yourself as:

```bash
$ gem install pigspec
```


## Usage

First: write a test case in your rspec code.
Sample:
```ruby
require 'pigspec'
include PigSpec

describe 'SamplePigTest' do
  it 'sample test' do
    actual = pig do
      script <<-EOS
        in = LOAD 'inputfile' AS (query:chararray);
        out = LIMIT in 1;
        STORE out INTO 'outputfile';
      EOS
      with_args 'n=2'
      override 'in', %w(hoge hoge hoge)
      pickup 'out'
    end
    expect(actual).to eq(%w((hoge)))
  end
```

Second: Set environment variable `PIG_HOME` to Your pig installed directory, And Run.
```bash
export PIG_HOME=<your pig installed dir>
spec
```

## Contributing

1. Fork it ( https://github.com/[my-github-username]/pigspec/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
