package com.netflix.aegisthus.output;

import com.netflix.aegisthus.io.writable.RowWritable;
import com.netflix.aegisthus.util.CFMetadataUtility;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.CFDefinition;
import org.apache.cassandra.cql3.statements.ColumnGroupMap;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.ByteBuffer;


public class AvroOutputFormat extends CustomFileNameFileOutputFormat<BytesWritable, RowWritable> {
    @Override
    public RecordWriter<BytesWritable, RowWritable> getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
        final CFMetaData cfMetaData = CFMetadataUtility.initializeCfMetaData(context.getConfiguration());
        final CFDefinition cfDefinition = cfMetaData.getCfDef();

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

                // Tombstones?
                if (cgmBuilder.groups().size() == 0)
                    return;

                ColumnGroupMap group = cgmBuilder.firstGroup();

                // Partition columns
                for (CFDefinition.Name name : cfDefinition.partitionKeys()) {
                    System.out.print(name);
                    System.out.print(" ");
                    System.out.println(keyComponents[name.position].toString());
                }

                // Clustering columns
                for (CFDefinition.Name name : cfDefinition.clusteringColumns()) {
                    System.out.print(name.name.toString());
                    System.out.print(" ");
                    System.out.println(group.getKeyComponent(name.position));
                }

                // Regular columns
                System.out.println("Regular columns");
                for (CFDefinition.Name name : cfDefinition.regularColumns()) {
                    System.out.println(name.name.toString());
                }

                // Static columns
                System.out.println("Static columns");
                for (CFDefinition.Name name : cfDefinition.staticColumns()) {
                    System.out.println(name.name.toString());
                }

            }

            @Override
            public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            }
        };
    }
}
