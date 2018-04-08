package com.siemens.opc.server;

public class ServerConfig {
    private final String applicationName;
    private final String serverName;
    private final Integer serverPort;

    public ServerConfig(String applicationName, String serverName, Integer serverPort) {
        this.applicationName = applicationName;
        this.serverName = serverName;
        this.serverPort = serverPort;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public String getServerName() {
        return serverName;
    }

    public Integer getServerPort() {
        return serverPort;
    }
}
