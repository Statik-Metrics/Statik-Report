package io.statik.report;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.logging.Level;

/**
 * Class to handle receiving messages.
 */
public class ReportHandler extends ChannelInboundHandlerAdapter {

    private final ReportServer rs;
    private final MessageHandler mh;

    /**
     * Creates a new ReportHandler and initializes its {@link io.statik.report.MessageHandler}.
     *
     * @param rs ReportServer this is running from
     */
    public ReportHandler(final ReportServer rs) {
        this.rs = rs;
        this.mh = new MessageHandler(rs);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        final SocketAddress sa = ctx.channel().remoteAddress();
        if (!(sa instanceof InetSocketAddress)) return;
        final InetSocketAddress isa = (InetSocketAddress) sa;
        Client c;
        synchronized (this.rs.getClients()) {
            c = this.rs.getClient(isa);
        }
        if (c == null) c = new Client(this.rs, isa);
        if (c.getStage() == Stage.NO_DATA) {
            ctx.writeAndFlush(ByteBufUtil.encodeString(ctx.alloc(), CharBuffer.wrap("No data should be sent."), Charset.forName("UTF-8")));
            return;
        }
        try {
            final Object write = this.mh.handleMessage(msg, c);
            final ByteBuf bb;
            if (write instanceof String) {
                bb = ByteBufUtil.encodeString(ctx.alloc(), CharBuffer.wrap((String) write), Charset.forName("UTF-8"));
            } else if (write instanceof ByteBuf) {
                bb = (ByteBuf) write;
            } else return;
            ctx.writeAndFlush(bb);
        } catch (final Throwable t) {
            this.rs.getLogger().warning("An exception occurred while reading a request:");
            this.rs.getLogger().log(Level.WARNING, t.getMessage(), t);
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    public static enum Stage {
        INTRODUCTION,
        DATA,
        NO_DATA
    }
}
