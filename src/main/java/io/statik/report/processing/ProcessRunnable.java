package io.statik.report.processing;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.trendrr.beanstalk.BeanstalkClient;
import com.trendrr.beanstalk.BeanstalkException;
import com.trendrr.beanstalk.BeanstalkJob;
import io.statik.report.ReportServer;
import io.statik.report.Request;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.logging.Level;

public class ProcessRunnable implements Runnable {

    private final ReportServer rs;
    final BeanstalkClient bsc;
    private final String collection;
    private volatile boolean running = true;

    public ProcessRunnable(final ReportServer instance) {
        this.rs = instance;
        this.bsc = this.rs.getNewBeanstalkClient();
        this.collection = this.rs.getConfiguration().getString("config.database.collections.data", null);
    }

    public void process() {
        final BeanstalkJob bsj;
        try {
            bsj = this.bsc.reserve(null); // wait indefinitely for a job
            if (bsj == null) return;
        } catch (final BeanstalkException ex) {
            this.rs.getLogger().warning("Could not reserve a BeanstalkJob:");
            this.rs.getLogger().log(Level.WARNING, ex.getMessage(), ex);
            ex.printStackTrace();
            return;
        }
        final DB db = this.rs.getMongoDB().getDB();
        db.requestStart();
        try {
            db.requestEnsureConnection();
            final Request r = new Request(new JSONObject(new String(bsj.getData()))); // we should be passed a JSONObject in String form
            final DBCollection dbc = db.getCollection(this.collection);
            dbc.insert(r.createMongoVersion());
        } catch (final JSONException ex) {
            this.rs.getLogger().warning("A JSONException occurred while processing data:");
            this.rs.getLogger().log(Level.WARNING, ex.getMessage(), ex);
        } finally {
            db.requestDone();
        }
        try {
            this.bsc.deleteJob(bsj);
        } catch (final BeanstalkException ex) {
            this.rs.getLogger().warning("Could not delete beanstalk job:");
            this.rs.getLogger().log(Level.WARNING, ex.getMessage(), ex);
        }
    }

    @Override
    public void run() {
        while (this.running) {
            this.process();
        }
        this.bsc.close();
    }

    public void setRunning(final boolean running) {
        this.running = running;
    }

}
