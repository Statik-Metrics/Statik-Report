package io.statik.report;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Request extends Configuration {

    public Request(final JSONObject request) {
        super(request);
    }

    /**
     * Sanitizes the given array of plugins.
     *
     * @param jsonPlugins JSONArray containing plugins (may be empty)
     * @return Sanitized JSONArray
     */
    private JSONArray createPluginList(final JSONArray jsonPlugins) {
        final JSONArray plugins = new JSONArray();
        for (int i = 0; i < jsonPlugins.length(); i++) {
            final JSONObject jo = jsonPlugins.optJSONObject(i);
            if (jo == null) continue;
            final JSONObject plugin = new JSONObject();
            plugin
                .put("name", jo.getString("name"))
                .put("version", jo.getString("version"));
            if (jo.has("data")) {
                final JSONArray jsonPluginCustomData = jo.getJSONArray("data");
                final JSONArray pluginCustomData = new JSONArray();
                for (int ii = 0; ii < jsonPluginCustomData.length(); ii++) {
                    final JSONObject customData = jsonPluginCustomData.optJSONObject(ii);
                    if (customData == null) continue;
                    pluginCustomData.put(new JSONObject()
                            .put("name", customData.getString("name"))
                            .put("value", customData.get("value"))
                    );
                }
                plugin.put("data", pluginCustomData);
            }
            plugins.put(plugin);
        }
        return plugins;
    }

    /**
     * Creates a MongoDB-compatible list of plugins and their data from a JSONArray.
     *
     * @param jsonPlugins JSONArray containing plugins (may be empty)
     * @return BasicDBList (never null)
     */
    private BasicDBList mongoPluginList(final JSONArray jsonPlugins) {
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
     * Converts this Request to a MongoDB version. Be sure to sanitize the Request before doing this.
     *
     * @return DBObject or null
     */
    public DBObject createMongoVersion() {
        final Object o = JSON.parse(this.getConfigRoot().toString());
        return o instanceof DBObject ? (DBObject) o : null;
    }

    /**
     * Creates a new Request with only official keys.
     *
     * @return New, sanitized Request
     * @throws org.json.JSONException If any key is missing or invalid
     */
    public Request sanitize() throws JSONException {
        final JSONObject system = this.getJSONObject("system");
        final JSONObject systemOS = this.getJSONObject("system.os");
        final JSONObject minecraft = this.getJSONObject("minecraft");
        final JSONObject minecraftMod = this.getJSONObject("minecraft.mod");
        return new Request(new JSONObject()
            .put("system", new JSONObject()
                    .put("java", system.getString("java"))
                    .put("cores", system.getInt("cores"))
                    .put("memory", system.getLong("memory"))
                    .put("os", new JSONObject()
                            .put("name", systemOS.getString("name"))
                            .put("version", systemOS.getString("version"))
                            .put("arch", systemOS.getString("arch"))
                    )
            )
            .put("minecraft", new JSONObject()
                    .put("version", minecraft.getString("version"))
                    .put("players", minecraft.getInt("players"))
                    .put("online_mode", minecraft.getBoolean("online_mode"))
                    .put("mod", new JSONObject()
                            .put("name", minecraftMod.getString("name"))
                            .put("version", minecraftMod.getString("version"))
                    )
            )
            .put("plugins", this.createPluginList(this.getJSONArray("plugins"))));
    }

    @Override
    public String toString() {
        return this.getConfigRoot().toString();
    }
}

