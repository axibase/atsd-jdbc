package com.axibase.tsd.driver.jdbc.protocol;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class NoopHostnameVerifier implements HostnameVerifier {
    static final NoopHostnameVerifier INSTANCE = new NoopHostnameVerifier();

    @Override
    public boolean verify(String s, SSLSession sslSession) {
        return true;
    }

    @Override
    public final String toString() {
        return "NO_OP";
    }
}
