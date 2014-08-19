package io.statik.report;

import io.statik.report.ReportHandler.Stage;

import java.net.InetSocketAddress;
import java.util.UUID;

public class Client {

    private final UUID uuid = UUID.randomUUID();
    private final ReportServer rs;
    private final InetSocketAddress remoteAddress;
    private UUID serverUUID = null;
    private Stage stage;
    private Request request;

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

    public Request getCurrentRequest() {
        return this.request;
    }

    public InetSocketAddress getRemoteAddress() {
        return this.remoteAddress;
    }

    public UUID getServerUUID() {
        return this.serverUUID;
    }

    /**
     * This may only be called once, as a Client should only be reporting for one server.
     *
     * @param serverUUID UUID of server this Client is reporting for
     * @throws java.lang.IllegalStateException If the serverUUID has already been set
     */
    public void setServerUUID(UUID serverUUID) {
        if (this.getServerUUID() != null) throw new IllegalStateException("serverUUID has already been set.");
        this.serverUUID = serverUUID;
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
