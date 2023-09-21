package com.pinterest.psc.interceptor;

import com.pinterest.psc.config.PscConfigurationInternal;

import java.util.List;

public class Interceptors<K, V> {
    List<TypePreservingInterceptor<byte[], byte[]>> rawDataInterceptors;
    List<TypePreservingInterceptor<K, V>> typedDataInterceptors;
    PscConfigurationInternal pscConfigurationInternal;

    public Interceptors(
            List<TypePreservingInterceptor<byte[], byte[]>> rawDataInterceptors,
            List<TypePreservingInterceptor<K, V>> typedDataInterceptors,
            PscConfigurationInternal pscConfigurationInternal
    ) {
        if (rawDataInterceptors != null)
            rawDataInterceptors.forEach(interceptor -> interceptor.setPscConfigurationInternal(pscConfigurationInternal));
        if (typedDataInterceptors != null)
            typedDataInterceptors.forEach(interceptor -> interceptor.setPscConfigurationInternal(pscConfigurationInternal));
        this.rawDataInterceptors = rawDataInterceptors;
        this.typedDataInterceptors = typedDataInterceptors;
        this.pscConfigurationInternal = pscConfigurationInternal;
    }

    public void addRawDataInterceptor(TypePreservingInterceptor<byte[], byte[]> interceptor) {
        interceptor.setPscConfigurationInternal(pscConfigurationInternal);
        rawDataInterceptors.add(interceptor);
    }

    public void addTypedDataInterceptor(TypePreservingInterceptor<K, V> interceptor) {
        interceptor.setPscConfigurationInternal(pscConfigurationInternal);
        typedDataInterceptors.add(interceptor);
    }

    public List<TypePreservingInterceptor<byte[], byte[]>> getRawDataInterceptors() {
        return rawDataInterceptors;
    }

    public List<TypePreservingInterceptor<K, V>> getTypedDataInterceptors() {
        return typedDataInterceptors;
    }

    public PscConfigurationInternal getPscConfigurationInternal() {
        return pscConfigurationInternal;
    }
}
