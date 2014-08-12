package io.statik.report.processing;

import io.statik.report.ReportServer;

public class ProcessThread extends Thread {

    public ProcessThread(final ReportServer instance) {
        super(new ProcessRunnable(instance));
    }

}
