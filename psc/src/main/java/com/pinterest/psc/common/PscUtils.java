package com.pinterest.psc.common;

import com.pinterest.psc.exception.startup.ConfigurationException;
import com.pinterest.psc.logging.PscLogger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
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
        return doesEc2MetadataExist() || isSysVendorAws() || IsAwsOsDetected();
    }

    protected static boolean doesEc2MetadataExist() {
        try {
            ProcessBuilder processBuilder = new ProcessBuilder("ec2metadata");
            processBuilder.redirectErrorStream(true);
            Process process = processBuilder.start();
            return process.waitFor() == 0;
        } catch (Exception e) {
            logger.info("Could not detect if host is EC2 from ec2metadata.", e);
            return false;
        }
    }

    protected static boolean isSysVendorAws() {
        try {
            return getFileContent("/sys/devices/virtual/dmi/id/sys_vendor").trim().equals("Amazon EC2");
        } catch (Exception e) {
            logger.info("Could not detect if host is EC2 from sys vendor.", e);
            return false;
        }
    }

    protected static boolean IsAwsOsDetected() {
        try {
            return System.getProperty("os.version").contains("aws");
        } catch (Exception e) {
            logger.info("Could not detect if host is EC2 from os version.", e);
            return false;
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

    public static String getFileContent(String path) throws IOException {
        StringBuilder content = new StringBuilder();
        BufferedReader reader = new BufferedReader(new FileReader(path));
        String line;
        while ((line = reader.readLine()) != null) {
            content.append(line).append(System.lineSeparator());
        }
        return content.toString();
    }
}
