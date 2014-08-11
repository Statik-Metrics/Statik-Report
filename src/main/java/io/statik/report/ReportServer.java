package io.statik.report;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.timeout.ReadTimeoutHandler;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class ReportServer {

    private final static Logger logger = Logger.getLogger("io.statik.report");
    private final Configuration c;
    private final MongoDB mdb;

    public ReportServer(final String configFileName) {
        this.setUpLogger();
        this.c = new Configuration(new File(configFileName));
        this.mdb = new MongoDB(this);
        final EventLoopGroup masterGroup = new NioEventLoopGroup();
        final EventLoopGroup slaveGroup = new NioEventLoopGroup();
        try {
            final ServerBootstrap sb = new ServerBootstrap();
            sb.group(masterGroup, slaveGroup).channel(NioServerSocketChannel.class);
            sb.option(ChannelOption.SO_BACKLOG, 128).childOption(ChannelOption.SO_KEEPALIVE, true);
            sb.childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(final SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new ReadTimeoutHandler(10, TimeUnit.SECONDS));
                    ch.pipeline().addLast(new StringEncoder(Charset.forName("UTF-8")));
                    ch.pipeline().addLast(new ReportHandler(ReportServer.this));
                    ch.pipeline().addLast(new StringDecoder(Charset.forName("UTF-8")));
                    ch.pipeline().addLast(new ExceptionHandler(ReportServer.this));
                }
            });
            final ChannelFuture cf = sb.bind(
                new InetSocketAddress(
                    this.getConfiguration().getString("config.bind.hostname", "localhost"),
                    this.getConfiguration().getInt("config.bind.port", 12345)
                )
            ).sync();
            cf.channel().closeFuture().sync();
        } catch (final Throwable t) {
            this.getLogger().severe("An exception was thrown during server setup:");
            this.getLogger().log(Level.SEVERE, t.getMessage(), t);
        } finally {
            masterGroup.shutdownGracefully();
            slaveGroup.shutdownGracefully();
        }
    }

    public static void main(final String[] args) {
        try {
            new ReportServer(args.length > 0 ? args[0] : "config.json");
        } catch (final Throwable t) {
            ReportServer.logger.severe("Could not start the report server due to the following exception: ");
            ReportServer.logger.log(Level.SEVERE, t.getMessage(), t);
            System.exit(1);
        }
    }

    private void setUpLogger() {
        final ConsoleHandler ch = new ConsoleHandler();
        ch.setFormatter(new Formatter() {
            @Override
            public String format(final LogRecord logRecord) {
                return "[" + logRecord.getLevel().getLocalizedName() + "] " + logRecord.getMessage() + "\n";
            }
        });
        this.getLogger().setUseParentHandlers(false);
        this.getLogger().addHandler(ch);
    }

    public Configuration getConfiguration() {
        return this.c;
    }

    public Logger getLogger() {
        return ReportServer.logger;
    }

    public MongoDB getMongoDB() {
        return this.mdb;
    }

}
