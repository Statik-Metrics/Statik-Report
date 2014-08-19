package io.statik.report;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.trendrr.beanstalk.BeanstalkClient;
import com.trendrr.beanstalk.BeanstalkException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.statik.report.ReportHandler.Stage;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;

import java.nio.charset.Charset;
import java.util.UUID;
import java.util.logging.Level;

/**
 * Class to process expected messages.
 */
public class MessageHandler {

    private final ReportServer rs;
    private final String timestampCollection;
    private final Charset utf8 = Charset.forName("UTF-8");
    private final String badContent = this.createErrorResponse("Bad content.");
    private final String illegalContent = this.createErrorResponse("The content provided was an illegal type.");
    private final String internalError = this.createErrorResponse("An internal error occurred whilst processing your data.");

    /**
     * Creates a new MessageHandler.
     *
     * @param rs ReportServer this is running from
     */
    public MessageHandler(final ReportServer rs) {
        this.rs = rs;
        this.timestampCollection = this.rs.getConfiguration().getString("config.database.collections.timestamps", null);
    }

    /**
     * Creates a ready-to-use error String for giving to the client.
     *
     * @param value Error message
     * @return String
     */
    private String createErrorResponse(final String value) {
        return new JSONStringer().object().key("error").value(value).endObject().toString();
    }

    private Status getStatus(final UUID serverUUID, final int version, final short waitTime) {
        if (version != 1) return Status.BAD_VERSION;
        else if (waitTime > (short) 0) return Status.WAIT;
        else return Status.GO_AHEAD;
    }

    private short getWaitTime(final UUID serverUUID) {
        final DB db = this.rs.getMongoDB().getDB();
        db.requestStart();
        try {
            db.requestEnsureConnection();
            final DBCollection dbc = db.getCollection(this.timestampCollection);
            final DBObject dbo = dbc.findOne(new BasicDBObject("uuid", serverUUID));
            if (dbo == null) return (short) 0; // this client has never sent before
            final Object timestampObject = dbo.get("timestamp");
            if (!(timestampObject instanceof Number)) return (short) 60;
            return (short) (((((Number) timestampObject).longValue() + 1800000L) - System.currentTimeMillis()) / (short) 1000);
        } catch (final MongoException ex) {
            this.rs.getLogger().log(Level.SEVERE, ex.getMessage(), ex);
        } finally {
            db.requestDone();
        }
        return (short) 60; // if some error happened
    }

    public String handleData(final ByteBuf bb, final Client client) {
        client.setStage(Stage.NO_DATA);
        final String message = bb.toString(this.utf8);
        try {
            final JSONObject jo = new JSONObject(message);
            return this.storeData(jo, client.getServerUUID());
        } catch (final JSONException ex) {
            return this.badContent;
        } catch (final Throwable t) {
            this.rs.getLogger().severe("An exception was thrown while handling a request:");
            this.rs.getLogger().log(Level.SEVERE, t.getMessage(), t);
            t.printStackTrace();
        }
        return this.internalError;
    }

    public ByteBuf handleIntroduction(final ByteBuf bb, final Client client) {
        final int version = bb.getInt(0);
        final UUID uuid = new UUID(bb.getLong(1), bb.getLong(2));
        if (client.getServerUUID() == null) client.setServerUUID(uuid);
        final byte[] badVersion = "Bad version".getBytes(Charset.forName("UTF-8"));
        final boolean isBadVersion = version != 1; // TODO: not hardcode this?
        final ByteBuf ret = Unpooled.buffer(isBadVersion ? 3 : 3 + badVersion.length);
        final short waitTime = this.getWaitTime(uuid);
        final Status status = isBadVersion ? Status.BAD_VERSION : this.getStatus(uuid, version, waitTime);
        ret.writeByte(status.getStatusByte());
        ret.writeShort(waitTime);
        if (status == Status.BAD_VERSION) {
            ret.writeBytes(badVersion);
            client.setStage(Stage.NO_DATA);
        } else if (status == Status.WAIT) {
            client.setStage(Stage.NO_DATA);
        } else client.setStage(Stage.DATA);
        return ret;
    }

    /**
     * Handles the given message. If msg is a ByteBuf, it will be processed into a JSONObject and attempted to be
     * stored.
     *
     * @param msg Message from a channel method
     * @return String to give back to the client
     */
    public Object handleMessage(final Object msg, final Client client) {
        if (!(msg instanceof ByteBuf)) return this.illegalContent;
        final ByteBuf bb = (ByteBuf) msg;
        switch (client.getStage()) {
            case INTRODUCTION:
                return this.handleIntroduction(bb, client);
            case DATA:
                return this.handleData(bb, client);
            default:
                return this.internalError;
        }
    }

    /**
     * Checks if the report data exists and stores it.
     *
     * @param jo Client's input
     * @return (JSON) String to be returned to client
     * @throws JSONException In case of any missing values
     */
    public String storeData(final JSONObject jo, final UUID uuid) throws JSONException {
        if (!this.rs.getConfiguration().pathExists("config.database.collections.data")) {
            this.rs.getLogger().warning("The data collection does not exist in the config.");
            return this.internalError;
        }
        if (this.timestampCollection == null) {
            this.rs.getLogger().warning("The timestamps collection does not exist in the config.");
            return this.internalError;
        }
        // Update (or insert if necessary) a timestamp tied to the server UUID, for reporting the time left to wait
        // before the client should send again.
        final DB db = this.rs.getMongoDB().getDB();
        db.requestStart();
        try {
            db.requestEnsureConnection();
            final DBCollection dbc = db.getCollection(this.timestampCollection);
            dbc.update(new BasicDBObject("uuid", uuid), new BasicDBObject("uuid", uuid).append("timestamp", System.currentTimeMillis()), true, false);
        } catch (final MongoException ex) {
            this.rs.getLogger().log(Level.SEVERE, ex.getMessage(), ex);
            return this.internalError;
        } finally {
            db.requestDone();
        }
        try {
            final Request r = new Request(jo).sanitize(); // will throw exception if invalid
            final BeanstalkClient bsc = this.rs.getNewBeanstalkClient();
            bsc.put(0L, 0, 5000, r.toString().getBytes(Charset.forName("UTF-8")));
            bsc.close();
        } catch (final JSONException ex) {
            return this.badContent;
        } catch (final IllegalArgumentException ex) {
            this.rs.getLogger().log(Level.SEVERE, ex.getMessage(), ex);
            return this.badContent;
        } catch (final BeanstalkException ex) {
            this.rs.getLogger().log(Level.SEVERE, ex.getMessage(), ex);
            return this.internalError;
        }
        final JSONStringer js = new JSONStringer();
        // TODO: Not this. Meaningful responses (next acceptable timestamp for new data)
        return js.object().key("result").value("Data queued for storage.").endObject().toString();
    }

    private enum Status {
        GO_AHEAD((byte) 0),
        BAD_VERSION((byte) 1),
        WAIT((byte) 2);

        private final byte statusByte;

        private Status(final byte statusByte) {
            this.statusByte = statusByte;
        }

        public byte getStatusByte() {
            return this.statusByte;
        }
    }

}
