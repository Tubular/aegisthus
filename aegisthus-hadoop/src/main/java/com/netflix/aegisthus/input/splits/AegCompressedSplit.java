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
    protected long dataLength;  // Size of decompressed data in bytes

    public static AegCompressedSplit createAegCompressedSplit(@Nonnull Path path, long start, long end, long dataLength, @Nonnull String[] hosts) {
        LOG.info("Split start: {}, end: {}, Total data size: {}", start, end, dataLength);

        AegCompressedSplit split = new AegCompressedSplit();
        split.path = path;
        split.start = start;
        split.end = end;
        split.hosts = hosts;
        split.dataLength = dataLength;
        return split;
    }

    @Nonnull
    @Override
    public InputStream getInput(@Nonnull Configuration conf) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream dataIn = fs.open(path);
        Path metadataPath = new Path(path.getParent(), path.getName().replaceAll("-Data.db", "-CompressionInfo.db"));
        FSDataInputStream metadataIn = fs.open(metadataPath);
        CompressionMetadata metadata = new CompressionMetadata(metadataIn, start, end, dataLength);
        return new CompressionInputStream(dataIn, metadata);
    }

    @Override
    public void write(@Nonnull DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(dataLength);
    }

    @Override
    public void readFields(@Nonnull DataInput in) throws IOException {
        super.readFields(in);
        this.dataLength = in.readLong();
    }
}
