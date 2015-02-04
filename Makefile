rebuild:
	./gradlew :aegisthus-hadoop:build

test:
	touch testing/output
	rm -R testing/output
	hadoop jar aegisthus-hadoop/build/libs/aegisthus-hadoop-0.3.0.jar org.coursera.SSTableExport -D aegisthus.cql_schema="CREATE TABLE test.test_table (bigint_field bigint, boolean_field boolean, float_field float, int_field int, list_int_field list<int>, set_text_field set<text>, map_time_field map<text,timestamp>, map_long_field map<text,bigint>, PRIMARY KEY ((bigint_field)));" -inputDir testing/test_table -output testing/output -avroSchemaFile testing/schema.avsc

check:
	java -jar testing/avro-tools-1.7.7.jar tojson testing/output/part-m-00000.avro
