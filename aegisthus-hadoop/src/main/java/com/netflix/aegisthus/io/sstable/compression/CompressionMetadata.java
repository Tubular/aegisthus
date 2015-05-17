package com.netflix.aegisthus.io.sstable.compression;

import com.google.common.collect.Maps;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class CompressionMetadata {
    private CompressionParameters parameters;
    private SortedMap<Long,Long> offsets;  // Association between compressed & uncompressed offsets
    private long compressedDataLength;
    private long decompressedDataLength;

    public CompressionMetadata(InputStream compressionInput, long dataLength) throws IOException {
        DataInputStream stream = new DataInputStream(compressionInput);

        parameters = readCompressionParameters(stream);
        decompressedDataLength = stream.readLong();
        compressedDataLength = dataLength;
        offsets = readChunksOffsets(stream);

        FileUtils.closeQuietly(stream);
    }

    public int chunkLength() {
        return parameters.chunkLength();
    }

    public ICompressor getCompressor() {
        return parameters.sstableCompressor;
    }

    public Pair<Long,Long> getCompressedBoundaries(long uncompressedOffset) {
        Pair<Long,Long> uncompressedBoundaries = getDecompressedBoundaries(uncompressedOffset);
        return Pair.of(offsets.get(uncompressedBoundaries.getLeft()), offsets.get(uncompressedBoundaries.getRight()));
    }

    public Pair<Long,Long> getDecompressedBoundaries(long uncompressedOffset) {
        long start = -1;
        long end = -1;

        for (long i: offsets.keySet()) {
            if (uncompressedOffset >= i)
                start = i;
            if (i > uncompressedOffset && end == -1)
                end = i;
        }
        return Pair.of(start, end);
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

    private SortedMap<Long,Long> readChunksOffsets(DataInput stream) throws IOException {
        SortedMap<Long,Long> extractedOffsets = new TreeMap<Long, Long>();
        int chunkCount = stream.readInt();

        for (long i = 0; i < chunkCount; i++)
            extractedOffsets.put(i * parameters.chunkLength(), stream.readLong());
        extractedOffsets.put(decompressedDataLength, compressedDataLength);

        return extractedOffsets;
    }
}
