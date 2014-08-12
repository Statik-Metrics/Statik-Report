package com.trendrr.beanstalk;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.NotYetConnectedException;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Wraps the beanstalk connection.
 *
 * @author dustin
 */
public class BeanstalkConnection {

    protected Logger log = Logger.getLogger("com.trendrr.beanstalk");

    private SocketChannel channel;
    private ByteArrayOutputStream outbuf = new ByteArrayOutputStream();

    private void throwException(Exception x) throws BeanstalkException {
        if (x instanceof NotYetConnectedException) throw new BeanstalkDisconnectedException(x);
        if (x instanceof IOException) throw new BeanstalkDisconnectedException(x);
        throw new BeanstalkException(x);
    }

    public void close() {
        try {
            outbuf.close();
        } catch (Exception x) {
            log.log(Level.FINER, "Caught", x);
        }
        try {
            channel.close();
        } catch (Exception x) {
            log.log(Level.FINER, "Caught", x);
        }
    }

    public void connect(String addr, int port) throws BeanstalkException {
        try {
            this.channel = SocketChannel.open();
            this.channel.connect(new InetSocketAddress(addr, port));
            this.channel.finishConnect();
        } catch (Exception x) {
            throw new BeanstalkException(x);
        }
    }

    @Override
    public void finalize() throws Throwable {
        super.finalize();
        this.close();
    }

    public boolean isOpen() {
        return channel != null && channel.isOpen();
    }

    public byte[] readBytes(int numBytes) throws BeanstalkException {
        byte[] bytes = new byte[numBytes];
        byte[] array = this.outbuf.toByteArray();
        this.outbuf = new ByteArrayOutputStream();
        int bytesWritten = 0;
        for (int i = 0; i < array.length; i++) {
            if (bytesWritten < bytes.length) {
                bytes[i] = array[i];
                bytesWritten++;
            } else this.outbuf.write(array[i]);
        }
        if (bytesWritten >= bytes.length) return bytes;
        int numRead = 1;
        while (numRead > 0) {
            ByteBuffer buf = ByteBuffer.allocate(4096);
            try {
                numRead = channel.read(buf);
            } catch (Exception x) {
                this.throwException(x);
            }
            byte[] read = buf.array();
            for (int i = 0; i < numRead; i++) {
                if (bytesWritten < bytes.length) bytes[bytesWritten] = read[i];
                else this.outbuf.write(read[i]);
                bytesWritten++;
            }
            if (bytesWritten >= bytes.length) {
                log.finer("468 GOT : " + bytesWritten + " " + bytes.length);
                return bytes;
            }
        }
        return bytes;
    }

    /**
     * returns the control response.  ends with \r\n
     *
     * @return
     */
    public String readControlResponse() throws BeanstalkException {
        String response = null;
        int count = 0;
        while (response == null) {
            count++;
            outbuf = new ByteArrayOutputStream();
            ByteBuffer buf = ByteBuffer.allocate(4096);
            if (count > 10000) {
                throw new BeanstalkException("Buffer has been empty for more than 100 seconds.");
            }
            try {
                if (channel.read(buf) == 0) {
                    log.warning("Nothing in the buffer, sleeping for 100 millis; will try again.");
                    try {
                        Thread.sleep(100);
                    } catch (Exception x) {
                        log.log(Level.SEVERE, "Caught", x);
                    }
                    continue;
                }
            } catch (Exception x) {
                this.throwException(x);
            }
            byte[] bytes = buf.array();
            ByteArrayOutputStream stringBuf = new ByteArrayOutputStream();
            byte lastByte = ' ';
            for (int i = 0; i < buf.position(); i++) {
                byte curByte = bytes[i];
                if (lastByte == '\r' && curByte == '\n' && response == null) {
                    response = new String(stringBuf.toByteArray()).trim();
                    if (response.isEmpty()) {
                        log.warning("Errant line end found, possibly from the previous request. Skipping.");
                        response = null;
                    }
                    continue;
                }
                if (response == null) stringBuf.write(curByte);
                else outbuf.write(curByte);
                lastByte = curByte;
            }
        }
        return response;
    }

    public void write(byte[] bytes) throws BeanstalkException {
        try {
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            while (buf.hasRemaining()) channel.write(buf);
        } catch (Exception x) {
            this.throwException(x);
        }
    }

    public void write(String str) throws BeanstalkException {
        try {
            ByteBuffer buf = ByteBuffer.wrap(str.getBytes());
            while (buf.hasRemaining()) channel.write(buf);
        } catch (Exception x) {
            this.throwException(x);
        }
    }
}
