package io.statik.report;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import io.netty.buffer.ByteBuf;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;

import java.nio.charset.Charset;
import java.util.logging.Level;

public class MessageHandler {

    private final ReportServer rs;
    private final String collection;
    private final Charset utf8 = Charset.forName("UTF-8");
    private final String badContent = this.createErrorResponse("Bad content.");
    private final String badVersion = this.createErrorResponse("Bad version.");
    private final String illegalContent = this.createErrorResponse("The content provided was an illegal type.");
    private final String internalError = this.createErrorResponse("An internal error occurred whilst processing your data.");

    public MessageHandler(final ReportServer rs) {
        this.rs = rs;
        this.collection = this.rs.getConfiguration().getString("config.database.collections.data", null);
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

    public BasicDBList addAndVerifyPlugins(final JSONArray jsonPlugins) {
        final BasicDBList plugins = new BasicDBList();
        for (int i = 0; i < jsonPlugins.length(); i++) {
            final JSONObject jo = jsonPlugins.optJSONObject(i);
            if (jo == null) continue;
            final BasicDBObject plugin = new BasicDBObject();
            plugin
                .append("name", jo.getString("name"))
                .append("version", jo.getString("version"));
            if (jo.has("data")) {
                final JSONArray jsonPluginCustomData = jo.getJSONArray("data");
                final BasicDBList pluginCustomData = new BasicDBList();
                for (int ii = 0; ii < jsonPluginCustomData.length(); ii++) {
                    final JSONObject customData = jsonPluginCustomData.optJSONObject(ii);
                    if (customData == null) continue;
                    pluginCustomData.add(new BasicDBObject()
                            .append("name", customData.getString("name"))
                            .append("value", customData.get("value"))
                    );
                }
                plugin.append("data", pluginCustomData);
            }
            plugins.add(plugin);
        }
        return plugins;
    }

    /**
     * Handles the given message. If msg is a ByteBuf, it will be processed into a JSONObject and attempted to be
     * stored.
     *
     * @param msg Message from a channel method
     * @return String to give back to the client
     */
    public String handleMessage(final Object msg) {
        if (!(msg instanceof ByteBuf)) return this.illegalContent;
        final ByteBuf bb = (ByteBuf) msg;
        final String message = bb.toString(this.utf8);
        try {
            final JSONObject jo = new JSONObject(message);
            final String version = jo.optString("version");
            if (!version.equalsIgnoreCase("1.0.0")) return this.badVersion;
            return this.storeData(jo);
        } catch (final JSONException ex) {
            return this.badContent;
        } catch (final Throwable t) {
            this.rs.getLogger().warning("An exception was thrown while handling a request:");
            this.rs.getLogger().log(Level.WARNING, t.getMessage(), t);
        }
        return this.internalError;
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
        final DB db = this.rs.getMongoDB().getDB();
        db.requestStart();
        try {
            db.requestEnsureConnection();
            final JSONObject system = jo.getJSONObject("system");
            final JSONObject systemOS = system.getJSONObject("os");
            final JSONObject minecraft = jo.getJSONObject("minecraft");
            final JSONObject minecraftMod = minecraft.getJSONObject("mod");
            final DBCollection dbc = db.getCollection(this.collection);
            final BasicDBObject insert = new BasicDBObject()
                .append("system", new BasicDBObject()
                        .append("java", system.getString("java"))
                        .append("cores", system.getInt("cores"))
                        .append("memory", system.getLong("memory"))
                        .append("os", new BasicDBObject()
                                .append("name", systemOS.getString("name"))
                                .append("version", systemOS.getString("version"))
                                .append("arch", systemOS.getString("arch"))
                        )
                )
                .append("minecraft", new BasicDBObject()
                        .append("version", minecraft.getString("version"))
                        .append("players", minecraft.getInt("players"))
                        .append("online_mode", minecraft.getBoolean("online_mode"))
                        .append("mod", new BasicDBObject()
                                .append("name", minecraftMod.getString("name"))
                                .append("version", minecraftMod.getString("version"))
                        )
                )
                .append("plugins", this.addAndVerifyPlugins(jo.getJSONArray("plugins")));
            dbc.insert(insert);
        } catch (final JSONException ex) {
            return this.badContent;
        } finally {
            db.requestDone();
        }
        final JSONStringer js = new JSONStringer();
        // TODO: Not this. Meaningful responses (next acceptable timestamp for new data)
        return js.object().key("result").value("Data stored").endObject().toString();
    }

}
