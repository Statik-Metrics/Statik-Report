package io.statik.report;

import com.trendrr.beanstalk.BeanstalkClient;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.statik.report.processing.ProcessThread;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * Main class. Runs the report server for Statik.
 */
public class ReportServer {

    private final static Logger logger = Logger.getLogger("io.statik.report");
    private final Configuration c;
    private final MongoDB mdb;
    private final List<Client> clients = Collections.synchronizedList(new ArrayList<Client>());

    /**
     * Starts the ReportServer with the given configuration file.
     *
     * @param configFileName File name to load as the configuration
     */
    public ReportServer(final String configFileName) {
        this.setUpLogger();
        this.c = new Configuration(new File(configFileName));
        this.mdb = new MongoDB(this);
        this.startBeanstalkProcessors();
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
                    ch.pipeline().addLast(new ReportHandler(ReportServer.this));
                    ch.pipeline().addLast(new EndOfTheLine(ReportServer.this));
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

    /**
     * Entry point. Creates a new {@link io.statik.report.ReportServer}, catching any exceptions.
     * <p/>
     * If any exception is caught, the application will exit with status 1.
     *
     * @param args Command-line arguments
     */
    public static void main(final String[] args) {
        try {
            new ReportServer(args.length > 0 ? args[0] : "config.json");
        } catch (final Throwable t) {
            ReportServer.logger.severe("Could not start the report server due to the following exception: ");
            ReportServer.logger.log(Level.SEVERE, t.getMessage(), t);
            System.exit(1);
        }
    }

    /**
     * Sets up the main {@link java.util.logging.Logger}.
     * <p/>
     * Example output: <code>[WARNING] Some message!</code>
     */
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

    /**
     * Starts a configurable amount of processors (if not configured, 4 is the default) in new threads. Processors sit
     * and wait for beanstalkd to feed them jobs. Once receiving the job, the processor will process it and store it in
     * the MongoDB.
     */
    private void startBeanstalkProcessors() {
        for (int i = 0; i < this.getConfiguration().getInt("config.beanstalkd.processors", 4); i++) {
            new ProcessThread(this).start();
        }
    }

    public Client getClient(final InetSocketAddress isa) {
        synchronized (this.getClients()) {
            for (final Client client : this.getClients()) {
                if (client.getRemoteAddress().equals(isa)) return client;
            }
        }
        return null;
    }

    public List<Client> getClients() {
        synchronized (this.clients) {
            return this.clients;
        }
    }

    /**
     * Gets the {@link io.statik.report.Configuration} this server is running with.
     *
     * @return Configuration
     */
    public Configuration getConfiguration() {
        return this.c;
    }

    /**
     * Gets the main {@link java.util.logging.Logger} for this server. This should be used whenever it is necessary to
     * output to the console.
     *
     * @return Logger
     */
    public Logger getLogger() {
        return ReportServer.logger;
    }

    /**
     * Gets the MongoDB link for this server.
     *
     * @return MongoDB
     */
    public MongoDB getMongoDB() {
        return this.mdb;
    }

    /**
     * Gets a new BeanstalkClient for immediate use.
     *
     * @return BeanstalkClient
     */
    public BeanstalkClient getNewBeanstalkClient() {
        return new BeanstalkClient(
            this.getConfiguration().getString("config.beanstalkd.hostname", null),
            this.getConfiguration().getInt("config.beanstalkd.port", -1),
            "processing"
        );
    }
}
