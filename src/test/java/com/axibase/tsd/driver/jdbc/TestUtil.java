package com.axibase.tsd.driver.jdbc;

import com.axibase.tsd.driver.jdbc.content.ContentMetadata;
import com.axibase.tsd.driver.jdbc.enums.AtsdType;
import com.axibase.tsd.driver.jdbc.util.EnumUtil;
import org.apache.calcite.avatica.ColumnMetaData;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.zip.ZipInputStream;

import static com.axibase.tsd.driver.jdbc.DriverConstants.DEFAULT_CHARSET;

public class TestUtil {
	private TestUtil() {
	}

	public static String resourceToString(String relativePath, Class<?> clazz) {
		try {
			final URL url = clazz.getResource(relativePath);
			if (url == null) {
				throw new IOException("File not found by given relative path");
			}
			final Path path = Paths.get(url.toURI());
			return new String(Files.readAllBytes(path), DEFAULT_CHARSET);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static InputStream getInputStreamForResource(String relativePathToResource, Class<?> clazz) throws IOException {
		final InputStream mockIs = clazz.getResourceAsStream(relativePathToResource);
		if (relativePathToResource.endsWith(".zip")) {
			ZipInputStream mockZip = new ZipInputStream(mockIs);
			mockZip.getNextEntry();
			return mockZip;
		}
		return mockIs;
	}

	public static List<ColumnMetaData> prepareMetadata(String relativePathToResource, Class<?> clazz) {
		try (InputStream inputStream = getInputStreamForResource(relativePathToResource, clazz);
			 BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))){
			return prepareMetadata(reader.readLine());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static List<ColumnMetaData> prepareMetadata(String header) {
		final boolean odbcCompatible = false;
		if (header.startsWith("#")) {
			return Collections.singletonList(ColumnMetaData.dummy(AtsdType.STRING_DATA_TYPE.getAvaticaType(odbcCompatible), false));
		} else {
			String[] columnNames = header.split(",");
			ColumnMetaData[] meta = new ColumnMetaData[columnNames.length];
			for (int i = 0; i < columnNames.length; i++) {
				final String columnName = columnNames[i];
				meta[i] = new ContentMetadata.ColumnMetaDataBuilder(false, odbcCompatible)
						.withName(columnName)
						.withLabel(columnName)
						.withColumnIndex(i)
						.withNullable(columnName.startsWith("tag") ? 1 : 0)
						.withAtsdType(EnumUtil.getAtsdTypeByColumnName(columnName))
						.build();
			}
			return Arrays.asList(meta);
		}
	}
}
