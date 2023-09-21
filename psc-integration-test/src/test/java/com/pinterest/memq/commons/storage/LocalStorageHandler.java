package com.pinterest.memq.commons.storage;

import com.codahale.metrics.MetricRegistry;
import com.google.gson.JsonObject;
import com.pinterest.memq.client.commons.Compression;
import com.pinterest.memq.core.commons.Message;
import org.apache.commons.compress.utils.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

@StorageHandlerName(name = "localstorage")
public class LocalStorageHandler implements StorageHandler {

    private static final AtomicInteger batchHeaderSize = new AtomicInteger();
    private static int numMessages = 100;
    private static byte[] data;

    static {
        BiFunction<String, Integer, byte[]> getLogMessageBytes = (base, k) -> base.getBytes();
        try {
            data = getMemqBatchData("testmsg", getLogMessageBytes, 100, numMessages, true,
                    Compression.NONE, null, false, batchHeaderSize);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getReadUrl() {
        return "";
    }

    public static AtomicInteger getBatchHeaderSize() {
        return batchHeaderSize;
    }

    @Override
    public void writeOutput(int arg0, int arg1, List<Message> arg2) throws WriteFailedException {
    }

    @Override
    public void initReader(Properties properties, MetricRegistry registry) throws Exception {
        numMessages = Integer.parseInt(properties.getProperty("num.messages", "100"));
    }

    @Override
    public InputStream fetchBatchStreamForNotification(JsonObject nextNotificationToProcess) throws IOException {
        return new ByteArrayInputStream(data);
    }

    public static byte[] getMemqBatchData(String baseLogMessage,
                                          BiFunction<String, Integer, byte[]> getLogMessageBytes,
                                          int logMessageCount,
                                          int msgs,
                                          boolean enableMessageId,
                                          Compression compression,
                                          List<byte[]> messageIdHashes,
                                          boolean enableTestHeaders,
                                          AtomicInteger batchHeaderSize) throws Exception {
        List<ByteBuffer> bufList = new ArrayList<>();
        for (int l = 0; l < msgs; l++) {
            byte[] rawData = TestUtils.createMessage(baseLogMessage, getLogMessageBytes, logMessageCount,
                    enableMessageId, compression, messageIdHashes, enableTestHeaders);
            bufList.add(ByteBuffer.wrap(rawData));
        }
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        InputStream buf = getBatchHeadersAsInputStream(bufList, batchHeaderSize);
        IOUtils.copy(buf, os);
        for (ByteBuffer byteBuffer : bufList) {
            os.write(byteBuffer.array());
        }
        System.out.println("Created:" + bufList.size() + " buffers");
        return os.toByteArray();
    }

    public static InputStream getBatchHeadersAsInputStream(final List<ByteBuffer> messages,
                                                           AtomicInteger headerSize) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(Integer.BYTES * 2 + // header length
                messages.size() * Integer.BYTES * 3 // index bytes 12 bytes per index entry
        );
        header.putInt(header.capacity() - Integer.BYTES);// header length

        TestUtils.writeMessageIndex(messages, header);
        byte[] array = header.array();
        headerSize.set(array.length);
        return new ByteArrayInputStream(array);
    }

}
