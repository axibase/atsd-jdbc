package com.axibase.tsd.driver.jdbc.protocol;

import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

/**
 * Socket factory that trusts all SSL connections.
 */
public class TrustAllSslSocketFactory extends SocketFactory {
    /**
     * should keep alives be sent
     */
    private static final boolean SO_KEEPALIVE = false;

    /**
     * is out of band in-line enabled
     */
    private static final boolean OOBINLINE = false;

    /**
     * should the address be reused
     */
    private static final boolean SO_REUSEADDR = false;

    /**
     * do not buffer send(s) iff true
     */
    private static final boolean TCP_NODELAY = true;

    /**
     * size of receiving buffer
     */
    private static final int SO_RCVBUF = 8192;

    /**
     * size of sending buffer iff needed
     */
    private static final int SO_SNDBUF = 1024;

    /**
     * read timeout in milliseconds
     */
    private static final int SO_TIMEOUT = 12000;

    /**
     * connect timeout in milliseconds
     */
    private static final int SO_CONNECT_TIMEOUT = 5000;

    /**
     * enabling lingering with 0-timeout will cause the socket to be
     * closed forcefully upon execution of close()
     */
    private static final boolean SO_LINGER = true;

    /**
     * amount of time to linger
     */
    private static final int LINGER = 0;
    private static final LoggingFacade logger = LoggingFacade.getLogger(TrustAllSslSocketFactory.class);
    private static final TrustAllSslSocketFactory INSTANCE = new TrustAllSslSocketFactory();

    private final SSLSocketFactory sslSocketFactory;

    private TrustAllSslSocketFactory() {
        TrustManager[] trustAllCerts = {new DummyTrustManager()};
        SSLSocketFactory factory = null;
        try {
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new SecureRandom());
            factory = sc.getSocketFactory();
        } catch (Exception e) {
            logger.error("", e);
        }
        this.sslSocketFactory = factory;
    }

    @Override
    public Socket createSocket() throws IOException {
        return applySettings(sslSocketFactory.createSocket());
    }

    @Override
    public Socket createSocket(InetAddress host, int port)
            throws IOException {
        return applySettings(sslSocketFactory.createSocket(host, port));
    }

    @Override
    public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
        return applySettings(sslSocketFactory.createSocket(address, port, localAddress, localPort));
    }

    @Override
    public Socket createSocket(String host, int port)
            throws IOException {
        return applySettings(sslSocketFactory.createSocket(host, port));
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
        return applySettings(sslSocketFactory.createSocket(host, port, localHost, localPort));
    }

    static SSLSocketFactory getDefaultSSLSocketFactory() {
        return INSTANCE.sslSocketFactory;
    }

    /**
     * Applies the current settings to the given socket.
     *
     * @param s Socket to apply the settings to
     * @return Socket the input socket
     */
    protected Socket applySettings(Socket s) {
        try {
            s.setKeepAlive(SO_KEEPALIVE);
            s.setOOBInline(OOBINLINE);
            s.setReuseAddress(SO_REUSEADDR);
            s.setTcpNoDelay(TCP_NODELAY);
            s.setOOBInline(OOBINLINE);

            s.setReceiveBufferSize(SO_RCVBUF);
            s.setSendBufferSize(SO_SNDBUF);
            s.setSoTimeout(SO_TIMEOUT);
            s.setSoLinger(SO_LINGER, LINGER);
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
        return s;
    }

    /** Implementation of {@link X509TrustManager} that trusts all
     * certificates. */
    private static class DummyTrustManager implements X509TrustManager {
        public X509Certificate[] getAcceptedIssuers() {
            return null;
        }

        public void checkClientTrusted(X509Certificate[] certs, String authType) {
            // do nothing
        }

        public void checkServerTrusted(X509Certificate[] certs, String authType) {
            // do nothing
        }
    }
}