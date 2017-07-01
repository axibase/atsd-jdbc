package com.axibase.tsd.driver.jdbc.protocol;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.axibase.tsd.driver.jdbc.content.ContentDescription;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import org.apache.calcite.avatica.org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.ArrayUtils;

import static com.axibase.tsd.driver.jdbc.DriverConstants.DEFAULT_CHARSET;

public class MetadataRetriever {
	private static final LoggingFacade logger = LoggingFacade.getLogger(MetadataRetriever.class);

	private static final String SCHEME_HEADER = "Link";
	private static final String START_LINK = "<data:application/csvm+json;base64,";
	private static final String END_LINK = ">; rel=\"describedBy\"; type=\"application/csvm+json\"";

	private static final byte[] ENCODED_JSON_SCHEME_BEGIN = "#eyJAY29udGV4dCI6".getBytes(DEFAULT_CHARSET); // #{"@context":
	private static final byte LINEFEED = (byte)'\n';
	private static final int BUFFER_SIZE = 1024;

	private MetadataRetriever() {
	}

	private static InputStream readJsonSchemeAndReturnRest(InputStream inputStream, ByteArrayOutputStream result,
	                                                       ContentDescription contentDescription) throws IOException {
		final byte[] buffer = new byte[BUFFER_SIZE];
		int length;
		while ((length = inputStream.read(buffer)) != -1) {
			final int index = ArrayUtils.indexOf(buffer, LINEFEED);
			if (index < 0) {
				result.write(buffer, 0, length);
			} else {
				result.write(buffer, 0, index);
				final byte[] decoded = Base64.decodeBase64(result.toByteArray());
				final String jsonScheme = new String(decoded, DEFAULT_CHARSET);
				contentDescription.setJsonScheme(jsonScheme);
				if (logger.isTraceEnabled()) {
					logger.trace("JSON scheme: " + jsonScheme);
				}
				result.reset();
				final int newSize = length - index - 1;
				return new ByteArrayInputStream(buffer, index + 1, newSize);
			}
		}
		return new ByteArrayInputStream(result.toByteArray());
	}

	public static InputStream retrieveJsonSchemeAndSubstituteStream(InputStream inputStream, ContentDescription contentDescription)
			throws IOException {
		try (ByteArrayOutputStream result = new ByteArrayOutputStream()) {
			int length;
			final int testHeaderLength = ENCODED_JSON_SCHEME_BEGIN.length;
			byte[] testHeader = new byte[testHeaderLength];
			length = inputStream.read(testHeader);
			if (length == -1) {
				throw new IOException("Stream is empty");
			}
			if (!Arrays.equals(testHeader, ENCODED_JSON_SCHEME_BEGIN)) {
				result.write(testHeader, 0, length);
				ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(result.toByteArray());
				return new SequenceInputStream(byteArrayInputStream, inputStream);
			}
			result.write(testHeader, 1, length - 1);

			InputStream readAfterScheme = readJsonSchemeAndReturnRest(inputStream, result, contentDescription);
			return new SequenceInputStream(readAfterScheme, inputStream);
		}
	}

	public static void retrieveJsonSchemeFromHeader(Map<String, List<String>> map, ContentDescription contentDescription) {
		printHeaders(map);
		List<String> list = map.get(SCHEME_HEADER);
		String value = list != null && !list.isEmpty() ? list.get(0) : null;
		if (value != null && value.startsWith(START_LINK) && value.endsWith(END_LINK)) {
			final String encoded = value.substring(START_LINK.length(), value.length() - END_LINK.length());
			String json = new String(Base64.decodeBase64(encoded), DEFAULT_CHARSET);
			if (logger.isTraceEnabled()) {
				logger.trace("JSON schema: " + json);
			}
			contentDescription.setJsonScheme(json);
		}
	}

	private static void printHeaders(Map<String, List<String>> map) {
		if (logger.isTraceEnabled()) {
			for (Map.Entry<String, List<String>> entry : map.entrySet()) {
				logger.trace("Key: {} Value: {} ", entry.getKey(), entry.getValue());
			}
		}
	}
}
