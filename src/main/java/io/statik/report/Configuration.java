package io.statik.report;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;

public class Configuration {

    private final JSONObject configRoot;

    public Configuration(final JSONObject configRoot) {
        this.configRoot = configRoot;
    }

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

    private String join(final Collection<String> strings, char delimiter) {
        return this.join(strings.toArray(new String[strings.size()]), delimiter, 0, strings.size());
    }

    private String join(final String[] strings, char delimiter, int start, int end) {
        if (start >= strings.length || end > strings.length || start < 0 || end < 0) {
            throw new IllegalArgumentException("Invalid indices: start(" + start + ") end(" + end + ")");
        }
        final StringBuilder sb = new StringBuilder();
        for (int i = start; i < end; i++) sb.append(strings[i]).append(delimiter);
        return sb.length() > 1 ? sb.substring(0, sb.length() - 1) : sb.toString();
    }

    public boolean getBoolean(final String path, final boolean def) {
        final JSONObject parent = this.getJSONObject(this.getParent(path));
        return parent.optBoolean(this.getLastNode(path), def);
    }

    public JSONObject getConfigRoot() {
        return this.configRoot;
    }

    public int getInt(final String path, final int def) {
        final JSONObject parent = this.getJSONObject(this.getParent(path));
        return parent.optInt(this.getLastNode(path), def);
    }

    public JSONObject getJSONObject(final String path) {
        if (path.equals("")) return this.configRoot;
        JSONObject buffer = this.configRoot;
        for (final String part : path.split("\\.")) {
            buffer = buffer.optJSONObject(part);
            if (buffer == null) throw new IllegalArgumentException("Invalid path: " + path);
        }
        return buffer;
    }

    public String getLastNode(final String path) {
        final String[] split = path.split("\\.");
        return split.length < 1 ? path : split[split.length - 1];
    }

    public String getParent(final String path) {
        final String[] split = path.split("\\.");
        return split.length < 1 ? path : this.join(split, '.', 0, split.length - 1);
    }

    public String getString(final String path, final String def) {
        final JSONObject parent = this.getJSONObject(this.getParent(path));
        return parent.optString(this.getLastNode(path), def);
    }

    public boolean pathExists(final String path) {
        try {
            return this.getJSONObject(this.getParent(path)).has(this.getLastNode(path));
        } catch (final JSONException | IllegalArgumentException ex) {
            return false;
        }
    }
}
