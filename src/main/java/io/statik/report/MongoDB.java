package io.statik.report;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

import java.net.UnknownHostException;

/**
 * Class to handle talking with MongoDB.
 */
public class MongoDB {

    private final ReportServer rs;
    private final MongoClient mc;
    private final String database;

    /**
     * Creates a new instance of this class and connects to the configured database.
     *
     * @param rs ReportServer this is running from
     */
    public MongoDB(final ReportServer rs) {
        this.rs = rs;
        final Configuration c = this.rs.getConfiguration();
        final ServerAddress sa;
        try {
            sa = new ServerAddress(
                c.getString("config.database.hostname", "localhost"),
                c.getInt("config.database.port", 27017)
            );
        } catch (final UnknownHostException ex) {
            throw new IllegalArgumentException("Invalid configuration for database", ex);
        }
        this.mc = new MongoClient(sa);
        if (!c.pathExists("config.database.database")) throw new IllegalArgumentException("Missing database");
        this.database = c.getString("config.database.database", null);
    }

    /**
     * Gets the configured database for data storage.
     *
     * @return DB
     */
    public DB getDB() {
        return this.mc.getDB(this.database);
    }

}
