package com.netflix.aegisthus.io.sstable.compression;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FSDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;

import static java.lang.Math.min;

/**
 * Encapsulates interaction with compressed SSTable
 */
public class CompressionInputStream extends InputStream {
    private static final Logger LOG = LoggerFactory.getLogger(CompressionInputStream.class);
    private boolean closed;

    // Suppliers
    private final CompressionMetadata cm;  // Data from *-CompressionInfo.db
    private final FSDataInputStream in;  // Data from *-Data.db

    // Global uncompressed offset in a SSTable stream
    private long position = 0;

    // Current compressed block
    private final byte[] compressedChunk;

    // Current decompressed block
    private final byte[] decompressedChunk;
    private long decompressedChunkStart = -1;
    private long decompressedChunkEnd = -1;

    public CompressionInputStream(FSDataInputStream in, CompressionMetadata cm) {
        this.in = in;
        this.cm = cm;
        this.compressedChunk = new byte[cm.chunkLength() * 2];
        this.decompressedChunk = new byte[cm.chunkLength() * 2];
    }

    @Override
    public int available() throws IOException {
        if (closed)
            return 0;

        if (decompressedChunkEnd < position)
            return 0;

        return (int) (decompressedChunkEnd - position);
    }

    @Override
    public void close() throws IOException {
        try {
            in.close();
        } finally {
            closed = true;
        }
    }

    @Override
    public int read() throws IOException {
        if (closed || (position > cm.getDataEnd())) {
            return -1;
        }

        if (position >= decompressedChunkEnd) {
            LOG.debug("The next position {}, chunk ends on {}, asking for the next", position, decompressedChunkEnd);
            decompressChunk();
        }

        return decompressedChunk[(int) (position++ - decompressedChunkStart)] & 0xFF;
    }

    @Override
    public int read(@Nonnull byte[] output, int offset, int length) throws IOException {
        if (closed || (position > cm.getDataEnd())) {
            return -1;
        }

        if (length == 0)
            return 0;

        if (position >= decompressedChunkEnd) {
            LOG.debug("The next position {}, chunk ends on {}, asking for the next", position, decompressedChunkEnd);
            decompressChunk();
        }

        int size = min(length, available());
        System.arraycopy(decompressedChunk, (int) (position - decompressedChunkStart), output, offset, size);
        position += size;
        return size;
    }

    @Override
    public long skip(long n) {
        position += n;
        return n;
    }

    private void decompressChunk() throws IOException {
        Pair<Long,Long> compressedBoundaries = cm.getCompressedBoundaries(position);
        Pair<Long,Long> decompressedBoundaries = cm.getDecompressedBoundaries(position);
        int compressedBoundariesLength = (int) (compressedBoundaries.getRight() - compressedBoundaries.getLeft() - 4);
        int decompressedBoundariesLength = (int) (decompressedBoundaries.getRight() - decompressedBoundaries.getLeft());

        LOG.debug("Trying to decompress chunk {} for position {}", compressedBoundaries, position);

        // Set boundaries for new chunk
        decompressedChunkStart = decompressedBoundaries.getLeft();
        decompressedChunkEnd = decompressedBoundaries.getRight();

        LOG.debug("Expected decompressed boundaries {}", decompressedBoundaries);

        // Read compressed chunk
        in.seek(compressedBoundaries.getLeft());
        in.readFully(compressedChunk, 0, compressedBoundariesLength);

        LOG.debug("Successfully read {} bytes for chunk {}", compressedBoundariesLength, compressedBoundaries);

        // Decompress a chunk
        int successfullyDecompressed = cm.getCompressor().uncompress(
                compressedChunk, 0, compressedBoundariesLength, decompressedChunk, 0
        );
        assert successfullyDecompressed == decompressedBoundariesLength;

        LOG.debug("Successfully decompressed {} bytes from chunk {}", successfullyDecompressed, decompressedBoundaries);
    }
}
