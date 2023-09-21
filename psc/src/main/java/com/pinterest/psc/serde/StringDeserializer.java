package com.pinterest.psc.serde;

import com.pinterest.psc.config.PscConfiguration;
import com.pinterest.psc.exception.consumer.DeserializerException;

import java.io.UnsupportedEncodingException;

public class StringDeserializer implements Deserializer<String> {
    private String encoding = "UTF8";

    @Override
    public void configure(PscConfiguration pscConfiguration, boolean isKey) {
        String configName = isKey ? "deserializer.key.encoding" : "deserializer.value.encoding";
        String encodingValue = pscConfiguration.getString(configName);
        if (encodingValue == null)
            encodingValue = pscConfiguration.getString("deserializer.encoding");
        if (encodingValue != null)
            encoding = encodingValue;
    }

    @Override
    public String deserialize(byte[] bytes) throws DeserializerException {
        try {
            if (bytes == null)
                return null;
            else
                return new String(bytes, encoding);
        } catch (UnsupportedEncodingException e) {
            throw new DeserializerException("Error when deserializing byte[] to string due to unsupported encoding " + encoding);
        }
    }
}
