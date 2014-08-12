package io.statik.report;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;

/**
 * Class for loading JSON configuration.
 * <p/>
 * Methods in this class should return null for invalid paths.
 */
public class Configuration {

    private final JSONObject configRoot;

    /**
     * Initializes this Configuration with the given configuration root object.
     *
     * @param configRoot The root of the configuration
     */
    public Configuration(final JSONObject configRoot) {
        this.configRoot = configRoot;
    }

    /**
     * Initializes this Configuration with the given file's contents, which are validated.
     *
     * @param file File containing JSON
     */
    public Configuration(final File file) {
        final List<String> lines;
        try {
            lines = Files.readAllLines(file.toPath(), Charset.forName("UTF-8"));
        } catch (final IOException ex) {
            throw new IllegalArgumentException("File could not be read: " + file.getAbsolutePath(), ex);
        }
        try {
            this.configRoot = new JSONObject(this.join(lines, '\n'));
        } catch (final JSONException ex) {
            throw new IllegalArgumentException("Given configuration file was invalid", ex);
        }
    }

    /**
     * Joins a collection of Strings by delimiter.
     *
     * @param strings   Strings to join
     * @param delimiter Delimiter to place between the Strings
     * @return Joined String
     */
    private String join(final Collection<String> strings, char delimiter) {
        return this.join(strings.toArray(new String[strings.size()]), delimiter, 0, strings.size());
    }

    /**
     * Joins an array of Strings by delimiter, starting at start (inclusive) and ending at end (exclusive).
     *
     * @param strings   Strings to join
     * @param delimiter Delimiter to place between the Strings
     * @param start     Start index (inclusive)
     * @param end       End index (exclusive)
     * @return Joined String
     */
    private String join(final String[] strings, char delimiter, int start, int end) {
        if (start >= strings.length || end > strings.length || start < 0 || end < 0) {
            throw new IllegalArgumentException("Invalid indices: start(" + start + ") end(" + end + ")");
        }
        final StringBuilder sb = new StringBuilder();
        for (int i = start; i < end; i++) sb.append(strings[i]).append(delimiter);
        return sb.length() > 1 ? sb.substring(0, sb.length() - 1) : sb.toString();
    }

    /**
     * Gets a boolean from the given path.
     *
     * @param path Path to get boolean from
     * @param def  Default value to use if the path cannot be found
     * @return boolean
     */
    public boolean getBoolean(final String path, final boolean def) {
        final JSONObject parent = this.getJSONObject(this.getParent(path));
        return parent == null ? def : parent.optBoolean(this.getLastNode(path), def);
    }

    /**
     * Gets the root of this Configuration.
     *
     * @return Root JSONObject
     */
    public JSONObject getConfigRoot() {
        return this.configRoot;
    }

    /**
     * Gets an integer from the given path.
     *
     * @param path Path to get integer from
     * @param def  Default value to use if the path cannot be found
     * @return int
     */
    public int getInt(final String path, final int def) {
        final JSONObject parent = this.getJSONObject(this.getParent(path));
        return parent == null ? def : parent.optInt(this.getLastNode(path), def);
    }

    /**
     * Gets a JSONArray at the given path.
     *
     * @param path Path to get the JSONArray from
     * @return JSONArray
     */
    public JSONArray getJSONArray(final String path) {
        final JSONObject jo = this.getJSONObject(this.getParent(path));
        return jo == null ? null : jo.optJSONArray(this.getLastNode(path));
    }

    /**
     * Gets a JSONObject at the given path.
     *
     * @param path Path to get JSONObject from
     * @return JSONObject
     */
    public JSONObject getJSONObject(final String path) {
        if (path.equals("")) return this.configRoot;
        JSONObject buffer = this.configRoot;
        for (final String part : path.split("\\.")) {
            buffer = buffer.optJSONObject(part);
            if (buffer == null) return null;
        }
        return buffer;
    }

    /**
     * Gets the last node in a given path.
     * <p/>
     * An input of <code>config.database.hostname</code> would yield <code>hostname</code> as the output.
     *
     * @param path Path to get last node from
     * @return Last node
     */
    public String getLastNode(final String path) {
        final String[] split = path.split("\\.");
        return split.length < 1 ? path : split[split.length - 1];
    }

    /**
     * Gets the parent node of the given path.
     * <p/>
     * An input of <code>config.database.hostname</code> would yield <code>config.database</code> as the output.
     *
     * @param path Path to get the parent node from
     * @return Parent node
     */
    public String getParent(final String path) {
        final String[] split = path.split("\\.");
        return split.length < 1 ? path : this.join(split, '.', 0, split.length - 1);
    }

    /**
     * Gets a String at the given path.
     *
     * @param path Path to get String from
     * @param def  Default value to use if the path cannot be found
     * @return String
     */
    public String getString(final String path, final String def) {
        final JSONObject parent = this.getJSONObject(this.getParent(path));
        return parent == null ? def : parent.optString(this.getLastNode(path), def);
    }

    /**
     * Checks to see if a path exists in this Configuration.
     *
     * @param path Path to check
     * @return true if the path exists, false if otherwise
     */
    public boolean pathExists(final String path) {
        try {
            final JSONObject parent = this.getJSONObject(this.getParent(path));
            return parent != null && parent.has(this.getLastNode(path));
        } catch (final JSONException | IllegalArgumentException ex) {
            return false;
        }
    }
}
