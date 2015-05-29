rebuild:
	./gradlew :aegisthus-hadoop:build

test:
	touch testing/output
	rm -R testing/output
	hadoop jar aegisthus-hadoop/build/libs/aegisthus-hadoop-0.3.2.jar com.netflix.Aegisthus -D aegisthus.avro.datetime_format="yyyy-MM-dd" -D aegisthus.cql_schema="CREATE TABLE test.test_table (bigint_field bigint, boolean_field boolean, float_field float, int_field int, list_int_field list<int>, set_text_field set<text>, map_time_field map<text,timestamp>, map_long_field map<text,bigint>, PRIMARY KEY ((bigint_field)));" -inputDir testing/test_table/ -output testing/output -avroSchema '{ "namespace": "tubular.avro", "type": "record", "name": "TestRecord", "fields": [ {"name": "bigint_field", "type": "long"}, {"name": "boolean_field", "type": ["null", "boolean"]}, {"name": "float_field", "type": ["null", "float"]}, {"name": "int_field", "type": ["null", "int"]}, {"name": "list_int_field", "type": {"type": "array", "items": "int"}}, {"name": "set_text_field", "type": {"type": "array", "items": "string"}}, {"name": "map_time_field", "type": {"type": "map", "values": "string"}}, {"name": "map_long_field", "type": {"type": "map", "values": "long"}} ] }'

trends:
	touch testing/output
	rm -R testing/output
	hadoop jar aegisthus-hadoop/build/libs/aegisthus-hadoop-0.3.2.jar com.netflix.Aegisthus -D aegisthus.blocksize=134217728 -D mapreduce.reduce.shuffle.input.buffer.percent=0.5 -D "aegisthus.cql_schema=CREATE TABLE trends.trends (id text, metric text, date timestamp, amount bigint, otype text, PRIMARY KEY (id, metric, date))" -D aegisthus.avro.datetime_format=yyyy-MM-dd -inputDir testing/fail/ -output testing/output "{\"type\": \"record\", \"namespace\": \"tubularlabs\", \"name\": \"trends\", \"fields\": [{\"type\": [\"null\", \"string\"], \"name\": \"id\"}, {\"type\": [\"null\", \"string\"], \"name\": \"metric\"}, {\"type\": [\"null\", \"string\"], \"name\": \"date\"}, {\"type\": [\"null\", \"long\"], \"name\": \"amount\"}, {\"type\": [\"null\", \"string\"], \"name\": \"otype\"}]}"

check:
	java -jar testing/avro-tools-1.7.7.jar tojson testing/output/part-00000
