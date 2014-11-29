IMPORT 'spec/pig/test_macro.pig';
in = LOAD 'inputfile' AS (query:chararray);
macro_in = LIMIT in 1;
out = test_macro(macro_in, 'query');
STORE out INTO 'outputfile';
