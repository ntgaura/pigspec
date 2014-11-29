in = LOAD 'inputfile' AS (query:chararray);
out = LIMIT in 1;
STORE out INTO 'outputfile';
