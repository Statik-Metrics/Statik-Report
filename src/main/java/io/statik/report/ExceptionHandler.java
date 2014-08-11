package io.statik.report;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.ReadTimeoutException;

import java.util.logging.Level;

/**
 * Class to handle uncaught exceptions in the pipeline.
 */
public class ExceptionHandler extends ChannelInboundHandlerAdapter {

    private final ReportServer rs;

    public ExceptionHandler(final ReportServer rs) {
        this.rs = rs;
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
        if (cause instanceof ReadTimeoutException) {
            ctx.close();
            return;
        }
        this.rs.getLogger().severe("An uncaught exception occurred somewhere in the pipeline:");
        this.rs.getLogger().log(Level.SEVERE, cause.getMessage(), cause);
    }

}
