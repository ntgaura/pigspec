DEFINE test_macro(rel, column) RETURNS RET {
    $RET = FOREACH $rel GENERATE CONCAT('testconcat_', $column);
};
