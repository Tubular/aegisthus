package com.netflix.aegisthus.input.splits;

import com.netflix.aegisthus.io.sstable.compression.CompressionInputStream;
import com.netflix.aegisthus.io.sstable.compression.CompressionMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;

public class AegCompressedSplit extends AegSplit {
    private static final Logger LOG = LoggerFactory.getLogger(AegCompressedSplit.class);
    protected Path compressedPath;
    protected long compressedLength;

    public static AegCompressedSplit createAegCompressedSplit(@Nonnull Path path,
            long start,
            long end,
            @Nonnull String[] hosts,
            @Nonnull Path compressedPath,
            long compressedLength) {
        AegCompressedSplit split = new AegCompressedSplit();
        split.path = path;
        split.start = start;
        split.end = end;
        split.hosts = hosts;
        split.compressedPath = compressedPath;
        split.compressedLength = compressedLength;
        LOG.info("start: {}, end: {}", start, split.end);

        return split;
    }

    @Nonnull
    @Override
    public InputStream getInput(@Nonnull Configuration conf) throws IOException {
        FileSystem fs = compressedPath.getFileSystem(conf);
        FSDataInputStream dataIn = fs.open(path);
        FSDataInputStream cmIn = fs.open(compressedPath);
        CompressionMetadata compressionMetadata = new CompressionMetadata(new BufferedInputStream(cmIn), compressedLength);
        return new CompressionInputStream(dataIn, compressionMetadata);
    }

    @Override
    public void readFields(@Nonnull DataInput in) throws IOException {
        super.readFields(in);
        compressedPath = new Path(WritableUtils.readString(in));
        compressedLength = in.readLong();
    }

    @Override
    public void write(@Nonnull DataOutput out) throws IOException {
        super.write(out);
        WritableUtils.writeString(out, compressedPath.toUri().toString());
        out.writeLong(compressedLength);
    }
}
