package com.axibase.tsd.driver.jdbc.util;

import lombok.experimental.UtilityClass;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

@UtilityClass
public class IOUtils {
    private static final int BUFFER_SIZE = 16 * 1024;

    public static byte[] inputStreamToByteArray(InputStream inputStream) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final int length = BUFFER_SIZE;
        byte[] buffer = new byte[length];
        int read;
        while ((read = inputStream.read(buffer, 0, length)) != -1) {
            byteArrayOutputStream.write(buffer, 0, read);
        }
        byteArrayOutputStream.flush();
        return byteArrayOutputStream.toByteArray();
    }
}
