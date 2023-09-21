package com.pinterest.psc.logging.log4j1;

import org.apache.log4j.FileAppender;
import org.apache.log4j.spi.LoggingEvent;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ContextDataEnabledAppender extends FileAppender {
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss ZZZ");
    private static final Date date = new Date();

    @Override
    protected void subAppend(LoggingEvent event) {
        date.setTime(event.getTimeStamp());
        event.setProperty("ts", sdf.format(date));
        super.subAppend(event);
    }
}
