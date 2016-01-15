package com.netflix.aegisthus.io.sstable.compression;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FSDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Map;

public class CompressionMetadata {
    private static final Logger LOG = LoggerFactory.getLogger(CompressionInputStream.class);

    private CompressionParameters parameters;
    private long compressedDataLength;  // Length of compressed sstable in bytes
    private long decompressedDataLength;  // Length of decompressed sstable in bytes
    private int offsetsCount;  // Total count of offsets that are available in CompressionInfo file
    private int firstOffsetIndex;  // Index of the first offset among all offsets available in CompressionInfo file
    private List<Long> offsets;  // Subset of offsets for compression blocks within a decompressed file in bytes

    public CompressionMetadata(FSDataInputStream compressionInput, long start, long end, long dataLength) throws IOException {
        this.parameters = readCompressionParameters(compressionInput);
        this.compressedDataLength = dataLength;
        this.decompressedDataLength = compressionInput.readLong();
        this.offsetsCount = compressionInput.readInt();
        this.firstOffsetIndex = (int) start / this.parameters.chunkLength();
        this.offsets = readChunksOffsets(compressionInput, start, end);
        FileUtils.closeQuietly(compressionInput);
    }

    public static long getDecompressedDataLength(FSDataInputStream compressionInput) throws IOException {
        CompressionMetadata metadata = new CompressionMetadata(compressionInput, 0, 0, 0);
        return metadata.getDataEnd();
    }

    public int chunkLength() {
        return parameters.chunkLength();
    }

    public ICompressor getCompressor() {
        return parameters.sstableCompressor;
    }

    public Pair<Long,Long> getCompressedBoundaries(long decompressedOffset) {
        int globalIndex = (int) decompressedOffset / chunkLength();
        int relativeIndex = globalIndex - firstOffsetIndex;

        long start;
        long end;

        try {
            start = this.offsets.get(relativeIndex);
        } catch (IndexOutOfBoundsException e) {
            start = 0;
        }

        try {
            end = this.offsets.get(relativeIndex + 1);
        } catch (IndexOutOfBoundsException e) {
            end = compressedDataLength;
        }

        return Pair.of(start, end);
    }

    public Pair<Long,Long> getDecompressedBoundaries(long decompressedOffset) {
        long start = decompressedOffset - (decompressedOffset % chunkLength());
        long end = Math.min(start + chunkLength(), getDataEnd());

        return Pair.of(start, end);
    }

    public int size() {
        return offsetsCount;
    }

    public long getDataEnd() {
        return decompressedDataLength;
    }

    private CompressionParameters readCompressionParameters(DataInput stream) throws IOException {
        String compressorName = stream.readUTF();
        int optionCount = stream.readInt();
        Map<String, String> options = Maps.newHashMap();

        for (int i = 0; i < optionCount; ++i) {
            String key = stream.readUTF();
            String value = stream.readUTF();
            options.put(key, value);
        }
        int chunkLength = stream.readInt();

        try {
            return new CompressionParameters(compressorName, chunkLength, options);
        } catch (ConfigurationException e) {
            throw new RuntimeException("Cannot create CompressionParameters for stored parameters", e);
        }
    }

    private List<Long> readChunksOffsets(FSDataInputStream stream, long start, long end) throws IOException {
        if (end == 0)
            return null;

        long firstChunkOffset = stream.getPos();
        long chunksToSkip = start / this.parameters.chunkLength();
        long chunksToRead = (end / this.parameters.chunkLength() + 1) - chunksToSkip;

        List<Long> extractedOffsets = Lists.newArrayListWithExpectedSize((int) chunksToRead);
        stream.seek(firstChunkOffset + chunksToSkip * 8);
        for (long i = 0; i <= chunksToRead; i++) {
            try {
                long next = stream.readLong();
                extractedOffsets.add(next);
            } catch (EOFException e) {
                LOG.debug("Reached the end of file");
            }
        }

        return extractedOffsets;
    }
}
