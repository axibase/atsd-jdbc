/*
* Copyright 2016 Axibase Corporation or its affiliates. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
* https://www.axibase.com/atsd/axibase-apache-2.0.pdf
*
* or in the "license" file accompanying this file. This file is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/
package com.axibase.tsd.driver.jdbc.protocol;

import com.axibase.tsd.driver.jdbc.content.ContentDescription;
import com.axibase.tsd.driver.jdbc.content.json.QueryDescription;
import com.axibase.tsd.driver.jdbc.content.json.SendCommandResult;
import com.axibase.tsd.driver.jdbc.enums.Location;
import com.axibase.tsd.driver.jdbc.enums.MetadataFormat;
import com.axibase.tsd.driver.jdbc.ext.AtsdConnectionInfo;
import com.axibase.tsd.driver.jdbc.ext.AtsdException;
import com.axibase.tsd.driver.jdbc.ext.AtsdRuntimeException;
import com.axibase.tsd.driver.jdbc.intf.IContentProtocol;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import com.axibase.tsd.driver.jdbc.util.JsonMappingUtil;
import org.apache.calcite.avatica.org.apache.commons.codec.binary.Base64;
import org.apache.calcite.avatica.org.apache.http.HttpHeaders;
import org.apache.calcite.avatica.org.apache.http.entity.ContentType;
import org.apache.commons.lang3.StringUtils;

import javax.net.ssl.*;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static com.axibase.tsd.driver.jdbc.DriverConstants.*;

public class SdkProtocolImpl implements IContentProtocol {
	private static final LoggingFacade logger = LoggingFacade.getLogger(SdkProtocolImpl.class);
	private static final String POST_METHOD = "POST";
	private static final String GET_METHOD = "GET";
	private static final String CONTEXT_INSTANCE_TYPE = "SSL";
	private static final int CHUNK_LENGTH = 100;

	private static final TrustManager[] DUMMY_TRUST_MANAGER = new TrustManager[]{new X509TrustManager() {
		@Override
		public X509Certificate[] getAcceptedIssuers() {
			return null;
		}
		@Override
		public void checkServerTrusted(X509Certificate[] certs, String authType) throws CertificateException {
		}
		@Override
		public void checkClientTrusted(X509Certificate[] certs, String authType) throws CertificateException {
		}
	}};

	private static final HostnameVerifier DUMMY_HOSTNAME_VERIFIER = new HostnameVerifier() {
		@Override
		public boolean verify(String urlHostName, SSLSession session) {
			if (!urlHostName.equalsIgnoreCase(session.getPeerHost()) && logger.isDebugEnabled()) {
				logger.debug("[doTrustToCertificates] URL host {} is different to SSLSession host {}", urlHostName,
						session.getPeerHost());
			}
			return true;
		}
	};

	private final ContentDescription contentDescription;
	private HttpURLConnection conn;
	private String atsdQueryId;
	private String queryId;

	public SdkProtocolImpl(final ContentDescription contentDescription) {
		this.contentDescription = contentDescription;
	}

	@Override
	public InputStream readInfo() throws AtsdException, GeneralSecurityException, IOException {
		contentDescription.addRequestHeadersForDataFetching();
		return executeRequest(GET_METHOD, 0, contentDescription.getEndpoint());
	}

	@Override
	public InputStream readContent(int timeoutMillis) throws AtsdException, GeneralSecurityException, IOException {
		contentDescription.addRequestHeadersForDataFetching();
		contentDescription.initDataFetchingContent();
		InputStream inputStream = null;
		try {
			inputStream = executeRequest(POST_METHOD, timeoutMillis, contentDescription.getEndpoint());
			if (MetadataFormat.EMBED == contentDescription.getMetadataFormat()) {
				inputStream = MetadataRetriever.retrieveJsonSchemeAndSubstituteStream(inputStream, contentDescription);
			}
		} catch (IOException e) {
			if (logger.isDebugEnabled()) {
				logger.debug("Metadata retrieving error", e);
			}
			if (queryId != null) { // queryId is set if cancel method is invoked from another thread
				throw new AtsdRuntimeException(prepareCancelMessage());
			}
			if (e instanceof SocketException) {
				throw e;
			}
		}
		return inputStream;
	}

	private String prepareCancelMessage() {
		if (atsdQueryId != null) {
			return "Query with driver-generated id=" + queryId +
					" has been cancelled. ATSD queryId is " + atsdQueryId;
		} else {
			return "Disconnect occurred while executing query with driver-generated id=" + queryId;
		}
	}

	@Override
	public void cancelQuery() throws AtsdException, GeneralSecurityException, IOException {
        contentDescription.addRequestHeadersForDataFetching();
        String cancelEndpoint = Location.CANCEL_ENDPOINT.getUrl(contentDescription.getInfo()) + '?' + QUERY_ID_PARAM_NAME + '=' + queryId;
		InputStream result = executeRequest(GET_METHOD, 0, cancelEndpoint);
		try {
			final QueryDescription[] descriptionArray = JsonMappingUtil.mapToQueryDescriptionArray(result);
			if (descriptionArray.length > 0) {
				atsdQueryId = descriptionArray[0].getAtsdQueryId();
				queryId = descriptionArray[0].getQueryId();
			}
		} catch (IOException e){
			if (logger.isDebugEnabled()) {
				logger.debug("Wrong query description format", e);
			}
			queryId = contentDescription.getQueryId();
		}
	}

	@Override
	public long writeContent(int timeoutMillis) throws AtsdException, GeneralSecurityException, IOException {
		contentDescription.addRequestHeader(HttpHeaders.ACCEPT, PLAIN_AND_JSON_MIME_TYPE);
		contentDescription.addRequestHeader(HttpHeaders.CONTENT_TYPE, ContentType.TEXT_PLAIN.getMimeType());
		long writeCount = 0;
		try {
			InputStream inputStream = executeRequest(POST_METHOD, timeoutMillis, contentDescription.getEndpoint());
			final SendCommandResult sendCommandResult = JsonMappingUtil.mapToSendCommandResult(inputStream);
			logger.trace("[response] content: {}", sendCommandResult);
			if (StringUtils.isNotEmpty(sendCommandResult.getError())) {
				throw new AtsdException("ATSD server error: " + sendCommandResult.getError());
			}
			writeCount = sendCommandResult.getSuccess();
			logger.debug("[response] success: {}", sendCommandResult.getSuccess());
		} catch (IOException e) {
			if (logger.isDebugEnabled()) {
				logger.debug("Data writing error", e);
			}
			if (queryId != null) {
				throw new AtsdRuntimeException(prepareCancelMessage());
			}
			if (e instanceof SocketException) {
				throw e;
			}
		}
		return writeCount;
	}

	@Override
	public void close() throws Exception {
		if (logger.isTraceEnabled()) {
			logger.trace("[SdkProtocolImpl#close]");
		}
		if (this.conn != null) {
			this.conn.disconnect();
		}
	}

	private InputStream executeRequest(String method, int queryTimeoutMillis, String url) throws AtsdException, IOException, GeneralSecurityException {
		if (logger.isDebugEnabled()) {
			logger.debug("[request] {} {}", method, url);
		}
		this.conn = getHttpURLConnection(url);
		if (contentDescription.getInfo().secure()) {
			doTrustToCertificates((HttpsURLConnection) this.conn);
		}
		setBaseProperties(method, queryTimeoutMillis);
		if (MetadataFormat.HEADER == contentDescription.getMetadataFormat()
				&& StringUtils.isEmpty(contentDescription.getJsonScheme())) {
			MetadataRetriever.retrieveJsonSchemeFromHeader(conn.getHeaderFields(), contentDescription);
		}
		long contentLength = conn.getContentLengthLong();
		if (logger.isDebugEnabled()) {
			logger.debug("[response] " + contentLength);
		}

		final boolean gzipped = COMPRESSION_ENCODING.equals(conn.getContentEncoding());
		final int code = conn.getResponseCode();
		InputStream body;
		if (code != HttpsURLConnection.HTTP_OK) {
			if (logger.isDebugEnabled()) {
				logger.debug("Response code: " + code);
			}
			if (code == HttpURLConnection.HTTP_UNAUTHORIZED) {
				throw new AtsdException("Wrong credentials provided");
			}
			body = conn.getErrorStream();
			if (code != HttpURLConnection.HTTP_BAD_REQUEST) {
				try {
					final String error = JsonMappingUtil.deserializeErrorObject(body);
					throw new AtsdRuntimeException(error);
				} catch (IOException e) {
					throw new AtsdRuntimeException("HTTP code " + code);
				}
			}
		} else {
			body = conn.getInputStream();
		}
		return gzipped ? new GZIPInputStream(body) : body;
	}

	private void setBaseProperties(String method, int queryTimeoutMillis) throws IOException {
		final String login = contentDescription.getInfo().user();
		final String password = contentDescription.getInfo().password();
		if (!StringUtils.isEmpty(login) && !StringUtils.isEmpty(password)) {
			final String basicCreds = login + ':' + password;
			final byte[] encoded = Base64.encodeBase64(basicCreds.getBytes());
			conn.setRequestProperty(HttpHeaders.AUTHORIZATION, AUTHORIZATION_TYPE + new String(encoded));
		}
		conn.setAllowUserInteraction(false);
		conn.setConnectTimeout(contentDescription.getInfo().connectTimeoutMillis());
		conn.setDoInput(true);
		conn.setInstanceFollowRedirects(true);
		final int readTimeoutInMillis = getQueryTimeoutMillis(queryTimeoutMillis, contentDescription.getInfo());
		conn.setReadTimeout(readTimeoutInMillis);
		conn.setRequestMethod(method);
		conn.setRequestProperty(HttpHeaders.CONNECTION, CONN_KEEP_ALIVE);
		conn.setRequestProperty(HttpHeaders.USER_AGENT, USER_AGENT);
		conn.setUseCaches(false);
		setAdditionalRequestHeaders(contentDescription.getRequestHeaders());
		if (method.equals(POST_METHOD)) {
			final String postContent = contentDescription.getPostContent();
			conn.setRequestProperty(HttpHeaders.ACCEPT_ENCODING, COMPRESSION_ENCODING);
			conn.setRequestProperty(HttpHeaders.CONTENT_LENGTH, "" + postContent.length());
			conn.setChunkedStreamingMode(CHUNK_LENGTH);
			conn.setDoOutput(true);
			if (logger.isDebugEnabled()) {
				logger.debug("[content] {}",  postContent);
			}
			try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream(), DEFAULT_CHARSET.name()))) {
				writer.write(postContent);
				writer.flush();
			}
		} else {
			conn.setRequestProperty(HttpHeaders.ACCEPT_ENCODING, DEFAULT_ENCODING);
		}
	}

	private static int getQueryTimeoutMillis(int timeoutMillis, AtsdConnectionInfo info) {
		return timeoutMillis == 0 ? info.readTimeoutMillis() : timeoutMillis;
	}

	private static HttpURLConnection getHttpURLConnection(String uri) throws IOException {
		final URL url = new URL(uri);
		return (HttpURLConnection) url.openConnection();
	}

	private void doTrustToCertificates(final HttpsURLConnection sslConnection) {
		final SSLContext sslContext;
		try {
			sslContext = SSLContext.getInstance(CONTEXT_INSTANCE_TYPE);
		} catch (NoSuchAlgorithmException e) {
			if (logger.isErrorEnabled()) {
				logger.error(e.getMessage());
			}
			return;
		}
		final boolean trusted = contentDescription.getInfo().trustCertificate();
		if (logger.isDebugEnabled()) {
			logger.debug("[doTrustToCertificates] {}", trusted);
		}
		try {
			sslContext.init(null, trusted ? DUMMY_TRUST_MANAGER : null, new SecureRandom());
		} catch (KeyManagementException e) {
			if (logger.isErrorEnabled()) {
				logger.error(e.getMessage());
			}
			return;
		}
		sslConnection.setSSLSocketFactory(sslContext.getSocketFactory());

		if (trusted) {
			sslConnection.setHostnameVerifier(DUMMY_HOSTNAME_VERIFIER);
		}
	}

	private void setAdditionalRequestHeaders(Map<String, String> headers) {
		for (Map.Entry<String, String> header : headers.entrySet()) {
			conn.setRequestProperty(header.getKey(), header.getValue());
		}
	}

}
