package io.statik.report;

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
    private final Charset utf8 = Charset.forName("UTF-8");
    private final String badContent = this.createErrorResponse("Bad content.");
    private final String badVersion = this.createErrorResponse("Bad version.");
    private final String illegalContent = this.createErrorResponse("The content provided was an illegal type.");
    private final String internalError = this.createErrorResponse("An internal error occurred whilst processing your data.");

    /**
     * Creates a new MessageHandler.
     *
     * @param rs ReportServer this is running from
     */
    public MessageHandler(final ReportServer rs) {
        this.rs = rs;
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

    private byte getStatus(final UUID serverUUID) {
        // TODO: check
        return (byte) 0;
    }

    public String handleData(final ByteBuf bb, final Client client) {
        client.setStage(Stage.NO_DATA);
        final String message = bb.toString(this.utf8);
        try {
            final JSONObject jo = new JSONObject(message);
            final String version = jo.optString("version");
            if (!version.equalsIgnoreCase("1.0.0")) return this.badVersion;
            return this.storeData(jo);
        } catch (final JSONException ex) {
            return this.badContent;
        } catch (final Throwable t) {
            this.rs.getLogger().severe("An exception was thrown while handling a request:");
            this.rs.getLogger().log(Level.SEVERE, t.getMessage(), t);
            t.printStackTrace();
        }
        return this.internalError;
    }

    public ByteBuf handleIntroduction(final ByteBuf bb, final Client c) {
        final int version = bb.getInt(0);
        final UUID uuid = new UUID(bb.getLong(1), bb.getLong(2));
        final byte[] badVersion = "Bad version".getBytes(Charset.forName("UTF-8"));
        final ByteBuf ret = Unpooled.buffer(version == 1 ? 3 : 3 + badVersion.length);
        final byte status = this.getStatus(uuid);
        // TODO: bad status if bad version
        ret.writeByte(status);
        ret.writeShort((short) 0);
        if (status != (byte) 0) {
            ret.writeBytes(badVersion);
            c.setStage(Stage.NO_DATA);
        } else c.setStage(Stage.DATA);
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
    public String storeData(final JSONObject jo) throws JSONException {
        if (!this.rs.getConfiguration().pathExists("config.database.collections.data")) return this.internalError;
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
            return this.internalError;
        }
        final JSONStringer js = new JSONStringer();
        // TODO: Not this. Meaningful responses (next acceptable timestamp for new data)
        return js.object().key("result").value("Data queued for storage.").endObject().toString();
    }

}
