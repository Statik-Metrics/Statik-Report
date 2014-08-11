package io.statik.report;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

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
        try {
            ctx.writeAndFlush(this.mh.handleMessage(msg));
        } catch (final Throwable t) {
            this.rs.getLogger().warning("An exception occurred while reading a request:");
            this.rs.getLogger().log(Level.WARNING, t.getMessage(), t);
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }
}
