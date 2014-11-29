DEFINE counting_macro(rel, column) RETURNS RET {
    rel_00 = GROUP $rel BY ($column);
    $RET = FOREACH rel_00 GENERATE group as word, COUNT($rel) AS count;
};
