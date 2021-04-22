package com.axibase.tsd.driver.jdbc.util;

import lombok.experimental.UtilityClass;

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

@UtilityClass
public class TimestampParseUtil {
    private static final long MILLIS_IN_SECOND = TimeUnit.SECONDS.toMillis(1);
    private static final long NANOS_IN_MILLIS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final long NANOS_IN_MICROS = TimeUnit.MICROSECONDS.toNanos(1);

    public static Timestamp parse(String millisStr) {
        final int dotIndex = millisStr.lastIndexOf('.');
        if (dotIndex == -1) {
            return new Timestamp(NumberParseUtil.parseLongFast(millisStr));
        }
        final long millis = NumberParseUtil.parseLongFast(millisStr, 0, dotIndex);
        final long microsPart = NumberParseUtil.parseLongFast(millisStr, dotIndex + 1, millisStr.length());
        final Timestamp timestamp = new Timestamp(millis);
        final int nanos = (int) ((millis % MILLIS_IN_SECOND) * NANOS_IN_MILLIS + microsPart * NANOS_IN_MICROS);
        timestamp.setNanos(nanos);
        return timestamp;
    }
}
