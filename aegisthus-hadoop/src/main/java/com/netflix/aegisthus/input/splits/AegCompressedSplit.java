package com.netflix.aegisthus.input.splits;

import com.netflix.aegisthus.io.sstable.compression.CompressionInputStream;
import com.netflix.aegisthus.io.sstable.compression.CompressionMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;

public class AegCompressedSplit extends AegSplit {
    private static final Logger LOG = LoggerFactory.getLogger(AegCompressedSplit.class);
    protected CompressionMetadata metadata;

    public static AegCompressedSplit createAegCompressedSplit(@Nonnull Path path,
            long start,
            long end,
            @Nonnull String[] hosts,
            CompressionMetadata metadata) {
        AegCompressedSplit split = new AegCompressedSplit();
        split.path = path;
        split.start = start;
        split.end = end;
        split.hosts = hosts;
        split.metadata = metadata.truncateTo(start, end);

        LOG.info("start: {}, end: {}, orig length: {}, compressed length: {}",
                start, end, metadata.size(), split.metadata.size());

        return split;
    }

    @Nonnull
    @Override
    public InputStream getInput(@Nonnull Configuration conf) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream dataIn = fs.open(path);
        return new CompressionInputStream(dataIn, metadata);
    }

    @Override
    public void readFields(@Nonnull DataInput in) throws IOException {
        super.readFields(in);
        metadata = CompressionMetadata.readObject(in);
    }

    @Override
    public void write(@Nonnull DataOutput out) throws IOException {
        super.write(out);
        metadata.writeObject(out);
    }
}
