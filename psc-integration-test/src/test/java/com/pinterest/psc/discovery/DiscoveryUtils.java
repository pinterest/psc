package com.pinterest.psc.discovery;

public class DiscoveryUtils {
    public static void overrideServersetPath(String newPath) {
        MockServersetServiceDiscoveryProvider.overrideServersetPath(newPath);
    }
}
