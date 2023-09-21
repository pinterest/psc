package com.pinterest.psc.common;

import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.exception.startup.ServiceDiscoveryException;
import com.pinterest.psc.logging.PscLogger;
import software.amazon.awssdk.core.SdkSystemSetting;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class PscUtils {
    private static final PscLogger logger = PscLogger.getLogger(PscUtils.class);
    public static String NO_TOPIC_URI = "n/a";
    public static int NO_PARTITION = -1;
    public static final Object lock = new Object();

    public static final String BACKEND_TYPE_KAFKA = "kafka";
    public static final String BACKEND_TYPE_MEMQ = "memq";

    public static <T> T instantiateFromClass(String fqdn, Class<T> targetClass) throws ConfigurationException {
        try {
            return Class.forName(fqdn).asSubclass(targetClass).newInstance();
        } catch (ClassNotFoundException e) {
            throw new ConfigurationException("Class not found: " + fqdn, e);
        } catch (IllegalAccessException | InstantiationException e) {
            throw new ConfigurationException("Could not create object from class: " + fqdn, e);
        }
    }

    public static boolean isEc2Host() {
        try {
            String hostAddressForEC2MetadataService = SdkSystemSetting.AWS_EC2_METADATA_SERVICE_ENDPOINT.getStringValueOrThrow();
            if (hostAddressForEC2MetadataService == null)
                return false;
            URL url = new URL(hostAddressForEC2MetadataService + "/latest/dynamic/instance-identity/document");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            con.setConnectTimeout(1000);
            con.setReadTimeout(1000);
            con.connect();
            con.disconnect();
            return con.getResponseCode() == 200;
        } catch (ConnectException connectException) {
            return isEc2HostAlternate();
        } catch (Exception exception) {
            logger.warn("Error occurred when determining the host type.", new ServiceDiscoveryException(exception));
            return false;
        }
    }

    protected static boolean isEc2HostAlternate() {
        ProcessBuilder processBuilder = new ProcessBuilder("ec2metadata");
        processBuilder.redirectErrorStream(true);
        try {
            Process process = processBuilder.start();
            return process.waitFor() == 0;
        } catch (IOException | InterruptedException e) {
            logger.info("Error occurred when running the `ec2metadata` command. Will check OS version as last resort.");
            return System.getProperty("os.version").contains("aws");
        }
    }

    public static String getVersion() {
        Properties properties = new Properties();
        try {
            properties.load(PscUtils.class.getClassLoader().getResourceAsStream("psc.properties"));
        } catch (IOException ioe) {
            logger.warn("Failed to get PSC version", ioe);
        }
        return properties.getProperty("version", "n/a");
    }

    public static String toString(Map map) {
        return map == null ? "null" : map.keySet().stream()
                .map(key -> "(" + key + ", " + map.get(key) + ")")
                .collect(Collectors.joining(", ", "[", "]")).toString();
    }

    public static String getStackTraceAsString(Exception exception) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        exception.printStackTrace(printWriter);
        return stringWriter.toString();
    }
}