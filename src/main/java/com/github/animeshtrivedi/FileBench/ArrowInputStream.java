package com.github.animeshtrivedi.FileBench;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

/**
 * Created by atr on 20.12.17.
 */
public class ArrowInputStream implements SeekableByteChannel {

    private FSDataInputStream instream;
    private long fileSize;
    private long truncatedSize;
    private boolean isOpen;

    public ArrowInputStream(FSDataInputStream instream, long fileSize){
        this.instream = instream;
        this.fileSize = fileSize;
        this.truncatedSize = fileSize;
        this.isOpen = true;
    }

    @Override
    final public int read(ByteBuffer dst) throws IOException {
        System.out.println("XXX: read " + dst.remaining() + " bytes");
        return this.instream.read(dst);
    }

    @Override
    final public int write(ByteBuffer src) throws IOException {
        throw new IOException("write call on read channel");
    }

    @Override
    final public long position() throws IOException {
        System.out.println("XXX: position get, at " + this.instream.getPos());
        return this.instream.getPos();
    }

    @Override
    final public SeekableByteChannel position(long newPosition) throws IOException {
        if(newPosition > this.truncatedSize){
            throw new IOException("Illegal seek, truncatedSize is " + this.truncatedSize +
                    " asked seek location " + newPosition +
                    " fileCapacity " + this.fileSize);
        }
        System.out.println("XXX: position set, at " + newPosition);
        this.instream.seek(newPosition);
        return this;
    }

    @Override
    final public long size() throws IOException {
        System.out.println("XXX: get size, at " + this.truncatedSize);
        return this.truncatedSize;
    }

    @Override
    final public SeekableByteChannel truncate(long size) throws IOException {
        System.out.println("XXX: set size, at " + size);
        this.truncatedSize = size;
        return null;
    }

    @Override
    final public boolean isOpen() {
        return isOpen;
    }

    @Override
    final public void close() throws IOException {
        this.instream.close();
        this.isOpen = false;
    }
}
