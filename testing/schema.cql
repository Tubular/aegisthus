CREATE TABLE test.test_table (
    bigint_field bigint,
    boolean_field boolean,
    float_field float,
    int_field int,
    list_int_field list<int>,
    set_text_field set<text>,
    map_time_field map<text,timestamp>,
    map_long_field map<text,bigint>,
    PRIMARY KEY ((bigint_field))
);
