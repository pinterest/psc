package com.pinterest.psc.discovery;

import com.pinterest.psc.common.ServiceDiscoveryConfig;
import com.pinterest.psc.common.TopicUri;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DiscoveryUtil {

    public static final String TMP_DIR = "/tmp/";

    public static void createTempServersetFile(String filename, List<String> content) throws IOException {
        File tempFile = new File(TMP_DIR, filename);
        tempFile.deleteOnExit();
        StringBuilder contentString = new StringBuilder();
        for (String line : content) {
            contentString.append(line).append("\n");
        }
        Files.write(tempFile.toPath(), contentString.toString().getBytes());
    }

    public static void createTempFallbackFile(String filename, Map<String, ServiceDiscoveryConfig> content)
            throws IOException {
        File tempFile = new File(TMP_DIR, filename);
        tempFile.deleteOnExit();
        StringBuilder contentString = new StringBuilder("{");
        boolean firstRound = true;
        for (Map.Entry<String, ServiceDiscoveryConfig> e : content.entrySet()) {
            if (!firstRound) {
                contentString.append(",\n");
            } else
                firstRound = false;
            contentString.append(String.format("  \"%s\": {\n", e.getKey()))
                    .append(String.format("    \"connect\": \"%s\",\n", e.getValue().getConnect()))
                    .append(String.format("    \"securityProtocol\": \"%s\"\n", e.getValue().getSecurityProtocol()))
                    .append("  }");
        }
        contentString.append("}");
        Files.write(tempFile.toPath(), contentString.toString().getBytes());
    }

    public static ServiceDiscoveryConfig createTestServiceDiscoveryConfig(String connect, String securityProtocol) {
        ServiceDiscoveryConfig config = new ServiceDiscoveryConfig();
        config.setConnect(connect);
        config.setSecurityProtocol(securityProtocol);
        return config;
    }

    /*
      Create temporary fallback file with the following format:
      ---------------------------------------------------------
      plaintext:/rn:kafka:env:aws_us-west-1::kafkacluster01:
        connect: "kafkacluster01001:9092,kafkacluster01002:9092"
        securityProtocol: "PLAINTEXT"
      plaintext:/rn:kafka:env:cloud_region-1::cluster:
        connect: "cluster01001:9092,cluster01002:9092"
        securityProtocol: "PLAINTEXT"
      plaintext:/rn:pubsubsys:env:cloud_region-1::cluster:
        connect: "cluster02001:9092,cluster02002:9092"
        securityProtocol: "PLAINTEXT"
      secure:/rn:pubsubsys:env:cloud_region-1::cluster:
        connect: "cluster01001:9093,cluster01002:9093"
        securityProtocol: "SSL"
      ---------------------------------------------------------
     */
    public static String createTempFallbackFile() throws IOException {
        String fallbackFilename = "discovery.json";
        Map<String, ServiceDiscoveryConfig> fallbackFileContent = new HashMap<>();
        fallbackFileContent.put(
                "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:aws_us-west-1::kafkacluster01:",
                DiscoveryUtil.createTestServiceDiscoveryConfig("kafkacluster01001:9092,kafkacluster01002:9092", "PLAINTEXT")
        );
        fallbackFileContent.put(
                "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region-1::cluster01:",
                DiscoveryUtil.createTestServiceDiscoveryConfig("cluster01001:9092,cluster01002:9092", "PLAINTEXT")
        );
        fallbackFileContent.put(
                "plaintext:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":pubsubsys:env:cloud_region-1::cluster02:",
                DiscoveryUtil.createTestServiceDiscoveryConfig("cluster02001:9092,cluster02002:9092", "PLAINTEXT")
        );
        fallbackFileContent.put(
                "secure:" + TopicUri.SEPARATOR + TopicUri.STANDARD + ":kafka:env:cloud_region-1::cluster01:",
                DiscoveryUtil.createTestServiceDiscoveryConfig("cluster01001:9093,cluster01002:9093", "SSL")
        );
        DiscoveryUtil.createTempFallbackFile(fallbackFilename, fallbackFileContent);
        return TMP_DIR + fallbackFilename;
    }
}
