package com.github.animeshtrivedi.FileBench;

import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Created by atr on 19.12.17.
 */
public class ArrowOutputStream implements WritableByteChannel {

    private FSDataOutputStream outStream;
    private Boolean isOpen;

    public ArrowOutputStream(FSDataOutputStream outStream){
        this.outStream = outStream;
        this.isOpen = true;
    }
    @Override
    public int write(ByteBuffer src) throws IOException {
        int remaining = src.remaining();
        if(src.isDirect()){
            // get the heap buffer directly and copy
            this.outStream.write(src.array());
            src.position(src.position() + remaining);
            return remaining;
        }
        throw new IOException(" Direct buffers are not imple");
    }

    @Override
    public boolean isOpen() {
        return this.isOpen;
    }

    @Override
    public void close() throws IOException {
        this.outStream.close();
        this.isOpen = false;
    }
}
