package com.netflix.aegisthus.output;

import com.netflix.aegisthus.io.writable.RowWritable;
import com.netflix.aegisthus.util.CFMetadataUtility;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.CFDefinition;
import org.apache.cassandra.cql3.statements.ColumnGroupMap;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;


public class AvroOutputFormat extends CustomFileNameFileOutputFormat<BytesWritable, RowWritable> {
    @Override
    public RecordWriter<BytesWritable, RowWritable> getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
        final CFMetaData cfMetaData = CFMetadataUtility.initializeCfMetaData(context.getConfiguration());
        final CFDefinition cfDefinition = cfMetaData.getCfDef();
        final Schema avroSchema = AvroJob.getOutputKeySchema(context.getConfiguration());
        final Path workFile = getDefaultWorkFile(context, "");
        final FSDataOutputStream outputStream = workFile.getFileSystem(context.getConfiguration()).create(workFile, false);
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(avroSchema);
        final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");  // To simplify queries in Hive

        dataFileWriter.create(avroSchema, outputStream);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        return new RecordWriter<BytesWritable, RowWritable>() {
            @Override
            public void write(BytesWritable keyWritable, RowWritable rowWritable) throws IOException, InterruptedException {
                // Split partition key on separate components
                ByteBuffer[] keyComponents = cfDefinition.hasCompositeKey
                        ? ((CompositeType) cfMetaData.getKeyValidator()).split(ByteBuffer.wrap(keyWritable.getBytes()))
                        : new ByteBuffer[] { ByteBuffer.wrap(keyWritable.getBytes()) };

                // Build column group map
                ColumnGroupMap.Builder cgmBuilder = new ColumnGroupMap.Builder(
                    (CompositeType) cfMetaData.comparator,
                    cfDefinition.hasCollections,
                    System.currentTimeMillis()
                );

                for (OnDiskAtom atom : rowWritable.getColumns()) {
                    if (!(atom instanceof Column))
                        continue;
                    cgmBuilder.add((Column) atom);
                }

                // No columns for partition key...
                if (cgmBuilder.groups().size() == 0)
                    return;

                for (ColumnGroupMap group: cgmBuilder.groups()) {
                    GenericRecord record = new GenericData.Record(avroSchema);

                    // Partition columns
                    for (CFDefinition.Name name : cfDefinition.partitionKeys()) {
                        addValue(record, name, keyComponents[name.position]);
                    }

                    // Clustering columns
                    for (CFDefinition.Name name : cfDefinition.clusteringColumns()) {
                        addValue(record, name, group.getKeyComponent(name.position));
                    }

                    // Regular columns
                    for (CFDefinition.Name name : cfDefinition.regularColumns()) {
                        addGroup(record, name, group);
                    }

                    dataFileWriter.append(record);
                }
            }

            @Override
            public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                dataFileWriter.close();
            }

            private void addValue(GenericRecord record, CFDefinition.Name name, ByteBuffer value) {
                record.put(name.name.toString(), getDeserializedValue(name.type, value));
            }

            private void addGroup(GenericRecord record, CFDefinition.Name name, ColumnGroupMap group) {
                if (name.type.isCollection()) {
                    CollectionType<?> collectionType = (CollectionType<?>) name.type;

                    if (collectionType.kind == CollectionType.Kind.MAP)
                        addMapValue(record, name, group, collectionType);
                    else if (collectionType.kind == CollectionType.Kind.LIST || collectionType.kind == CollectionType.Kind.SET)
                        addListValue(record, name, group, collectionType);

                } else {
                    Column c = group.getSimple(name.name.key);
                    addValue(record, name, (c == null) ? null : c.value());
                }
            }

            private void addMapValue(GenericRecord record, CFDefinition.Name name, ColumnGroupMap group, CollectionType<?> collectionType) {
                List<Pair<ByteBuffer, Column>> pairs = group.getCollection(name.name.key);
                if (pairs == null) {
                    record.put(name.name.toString(), new HashMap());
                    return;
                }

                Map<String, Object> map = new HashMap<String, Object>();

                for (Pair<ByteBuffer, Column> pair : pairs) {
                    if (pair == null)
                        continue;

                    String mapKey = collectionType.nameComparator().getString(pair.left);
                    Object mapValue = getDeserializedValue(collectionType.valueComparator(), pair.right.value());

                    if (mapValue == null)
                        continue;

                    map.put(mapKey, mapValue);
                }

                record.put(name.name.toString(), map);
            }

            private void addListValue(GenericRecord record, CFDefinition.Name name, ColumnGroupMap group, CollectionType<?> collectionType) {
                List<Pair<ByteBuffer, Column>> pairs = group.getCollection(name.name.key);
                if (pairs == null) {
                    record.put(name.name.toString(), new LinkedList());
                    return;
                }

                List<Object> list = new LinkedList<Object>();

                for (Pair<ByteBuffer, Column> pair : pairs) {
                    if (pair == null)
                        continue;

                    Object listValue;

                    if (collectionType.kind == CollectionType.Kind.LIST)
                        listValue = getDeserializedValue(collectionType.valueComparator(), pair.right.value());
                    else
                        listValue = getDeserializedValue(collectionType.nameComparator(), pair.left);

                    if (listValue == null)
                        continue;

                    list.add(listValue);
                }

                record.put(name.name.toString(), list);
            }

            private Object getDeserializedValue(AbstractType<?> type, ByteBuffer value) {
                if (value == null) {
                    return null;
                }

                Object valueDeserialized = type.compose(value);

                AbstractType<?> baseType = (type instanceof ReversedType<?>)
                        ? ((ReversedType<?>) type).baseType
                        : type;

                /* special case some unsupported CQL3 types to Hive types. */
                if (baseType instanceof UUIDType || baseType instanceof TimeUUIDType) {
                    valueDeserialized = ((UUID) valueDeserialized).toString();
                } else if (baseType instanceof BytesType) {
                    ByteBuffer buffer = (ByteBuffer) valueDeserialized;
                    byte[] data = new byte[buffer.remaining()];
                    buffer.get(data);

                    valueDeserialized = data;
                } else if (baseType instanceof TimestampType) {
                    Date date = (Date) valueDeserialized;
                    valueDeserialized = dateFormat.format(date);
                }

                return valueDeserialized;
            }
        };
    }
}
