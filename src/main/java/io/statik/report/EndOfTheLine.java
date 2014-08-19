package io.statik.report;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.ReadTimeoutException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.logging.Level;

/**
 * Class to handle uncaught exceptions in the pipeline and end of life situations.
 */
public class EndOfTheLine extends ChannelInboundHandlerAdapter {

    private final ReportServer rs;

    public EndOfTheLine(final ReportServer rs) {
        this.rs = rs;
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        final SocketAddress sa = ctx.channel().remoteAddress();
        ctx.close();
        if (!(sa instanceof InetSocketAddress)) return;
        final Client client = this.rs.getClient((InetSocketAddress) sa);
        if (client != null) client.destroy();
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
        // The client idled for too long, so we'll just close their channel and move on.
        // OR The client disconnected early
        // TODO: Fix ignoring all IOExceptions (some may be legitimate)
        if (cause instanceof ReadTimeoutException || cause instanceof IOException) {
            ctx.close();
            return;
        }
        this.rs.getLogger().severe("An uncaught exception occurred somewhere in the pipeline:");
        this.rs.getLogger().log(Level.SEVERE, cause.getMessage(), cause);
        cause.printStackTrace();
    }
}
