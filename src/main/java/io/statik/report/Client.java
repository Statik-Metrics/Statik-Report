package io.statik.report;

import io.statik.report.ReportHandler.Stage;

import java.net.InetSocketAddress;
import java.util.UUID;

public class Client {

    private final UUID uuid = UUID.randomUUID();
    private final ReportServer rs;
    private final InetSocketAddress remoteAddress;
    private Stage stage;

    public Client(final ReportServer rs, final InetSocketAddress remoteAddress) {
        this.rs = rs;
        this.remoteAddress = remoteAddress;
        this.stage = Stage.INTRODUCTION;
        synchronized (this.rs.getClients()) {
            this.rs.getClients().add(this);
        }
    }

    public void destroy() {
        this.rs.getClients().remove(this);
    }

    public InetSocketAddress getRemoteAddress() {
        return this.remoteAddress;
    }

    public Stage getStage() {
        return this.stage;
    }

    public void setStage(final Stage stage) {
        this.stage = stage;
    }

    public UUID getUUID() {
        return this.uuid;
    }
}
