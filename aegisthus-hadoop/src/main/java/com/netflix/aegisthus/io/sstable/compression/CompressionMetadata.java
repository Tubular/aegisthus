package com.netflix.aegisthus.io.sstable.compression;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CompressionMetadata {
    private CompressionParameters parameters;
    private List<Integer> offsets;
    private long compressedStart;
    private long decompressedStart;
    private long compressedDataLength;
    private long decompressedDataLength;

    public CompressionMetadata(InputStream compressionInput, long dataLength) throws IOException {
        DataInputStream stream = new DataInputStream(compressionInput);

        this.parameters = readCompressionParameters(stream);
        this.compressedStart = 0;
        this.decompressedStart = 0;
        this.decompressedDataLength = stream.readLong();
        this.compressedDataLength = dataLength;
        this.offsets = readChunksOffsets(stream);

        FileUtils.closeQuietly(stream);
    }

    public CompressionMetadata(CompressionParameters parameters, long compressedDataLength, long decompressedDataLength,
                               long compressedStart, long decompressedStart, List<Integer> offsets) {
        this.parameters = parameters;
        this.compressedStart = compressedStart;
        this.decompressedStart = decompressedStart;
        this.compressedDataLength = compressedDataLength;
        this.decompressedDataLength = decompressedDataLength;
        this.offsets = offsets;
    }

    public int chunkLength() {
        return parameters.chunkLength();
    }

    public ICompressor getCompressor() {
        return parameters.sstableCompressor;
    }

    public Pair<Long,Long> getCompressedBoundaries(long decompressedOffset) {
        int index = (int) ((decompressedOffset - decompressedStart) / chunkLength());
        long start = compressedStart;
        for (int i = 0; i < index; i++)
            start += offsets.get(i);
        long end = start + offsets.get(index);

        return Pair.of(start, end);
    }

    public Pair<Long,Long> getDecompressedBoundaries(long uncompressedOffset) {
        long start = uncompressedOffset - (uncompressedOffset % chunkLength());
        long end = start + chunkLength();

        return Pair.of(start, end);
    }

    public long getDataEnd() {
        return decompressedDataLength;
    }

    /**
     * Generated truncated metadata only for sub-chunk of data
     */
    public CompressionMetadata truncateTo(long uncompressedStart, long uncompressedEnd) {
        long uncompressedPosition = 0;
        long compressedPosition = 0;
        long compressedStart = 0;
        List<Integer> subOffsets = new ArrayList<Integer>();

        for (int offset: offsets) {
            uncompressedPosition += chunkLength();
            compressedPosition += offset;

            if (uncompressedPosition <= uncompressedStart)
                compressedStart = compressedPosition;
            else
                subOffsets.add(offset);

            if (uncompressedPosition > uncompressedEnd) {
                break;
            }
        }

        return new CompressionMetadata(
                parameters,
                compressedDataLength,
                decompressedDataLength,
                compressedStart,
                getDecompressedBoundaries(uncompressedStart).getLeft(),
                subOffsets
        );
    }

    public int size() {
        return offsets.size();
    }

    public void writeObject(DataOutput stream) throws IOException {
        CompressionParameters.serializer.serialize(parameters, stream, 1);

        stream.writeInt(offsets.size());
        for (int i: offsets) {
            stream.writeInt(i);
        }

        stream.writeLong(compressedStart);
        stream.writeLong(decompressedStart);
        stream.writeLong(compressedDataLength);
        stream.writeLong(decompressedDataLength);
    }

    public static CompressionMetadata readObject(DataInput stream) throws IOException {
        CompressionParameters dParameters = CompressionParameters.serializer.deserialize(stream, 1);

        int dCount = stream.readInt();
        List<Integer> dOffsets = Lists.newArrayListWithExpectedSize(dCount);
        for (int i = 0; i < dCount; i++)
            dOffsets.add(stream.readInt());

        long dCompressedStart = stream.readLong();
        long dDecompressedStart = stream.readLong();
        long dCompressedDataLength = stream.readLong();
        long dDecompressedDataLength = stream.readLong();

        return new CompressionMetadata(
                dParameters, dCompressedDataLength, dDecompressedDataLength,
                dCompressedStart, dDecompressedStart, dOffsets
        );
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

    private List<Integer> readChunksOffsets(DataInput stream) throws IOException {
        int chunkCount = stream.readInt();
        List<Integer> extractedOffsets = Lists.newArrayListWithExpectedSize(chunkCount);

        long prev = stream.readLong();
        long next;
        for (long i = 0; i < (chunkCount - 1); i++) {
            next = stream.readLong();
            extractedOffsets.add((int) (next - prev));
            prev = next;
        }
        extractedOffsets.add((int) (compressedDataLength - prev));

        return extractedOffsets;
    }
}
