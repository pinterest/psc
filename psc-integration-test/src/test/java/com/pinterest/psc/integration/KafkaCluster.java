package com.pinterest.psc.integration;

import com.google.common.io.Files;
import com.pinterest.psc.discovery.DiscoveryUtils;

import java.io.File;
import java.io.IOException;

public class KafkaCluster {
    private static final String TEST_SERVERSET_PATH = "/tmp/";
    private String transport, region, cluster;
    private int port;

    public KafkaCluster(String transport, String region, String cluster, int port) throws IOException {
        this.transport = transport;
        this.region = region;
        this.cluster = cluster;
        this.port = port;
        createServersetFile();
    }

    public String getTransport() {
        return transport;
    }

    public String getRegion() {
        return region;
    }

    public String getCluster() {
        return cluster;
    }

    public int getPort() {
        return port;
    }

    private void createServersetFile() throws IOException {
        Files.write(getBootstrap().getBytes(), new File(getServersetFileName(false)));
        new File(getServersetFileName(false)).deleteOnExit();
    }

    public void createTlsServersetFile() throws IOException {
        Files.write(getBootstrap().getBytes(), new File(getServersetFileName(true)));
        new File(getServersetFileName(true)).deleteOnExit();
    }

    public String getBootstrap() {
        return "localhost:" + port;
    }

    private String getServersetFileName(boolean tls) {
        DiscoveryUtils.overrideServersetPath(TEST_SERVERSET_PATH);
        if (tls) {
            return TEST_SERVERSET_PATH + String.format("%s.discovery.%s_tls.serverset", region, cluster);
        }
        return TEST_SERVERSET_PATH + String.format("%s.discovery.%s.serverset", region, cluster);
    }
}
