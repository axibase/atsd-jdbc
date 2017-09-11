package com.axibase.tsd.driver.jdbc.ext;

import com.axibase.tsd.driver.jdbc.DriverConstants;
import lombok.SneakyThrows;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@RunWith(Parameterized.class)
public class PrepareMetricUrlTest {
	private static final String METRICS_ENDPOINT = "https://localhost:8443/api/v1/metrics";
	private static final int INDEX_OF_EXPRESSION = (METRICS_ENDPOINT + "?expression=").length();

	private final List<String> patternsFromConnectionString;
	private final String tablesPattern;
	private final Object expectedExpression;

	public PrepareMetricUrlTest(List<String> patternsFromConnectionString, String tablesPattern, Object expectedExpression) {
		this.tablesPattern = tablesPattern;
		this.patternsFromConnectionString = patternsFromConnectionString;
		this.expectedExpression = expectedExpression;
	}

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
				{Collections.singletonList("%"), "my.metric", "name like 'my.metric'"},
				{Collections.singletonList("%"), "my_metric", "name like 'my?metric'"},
				{Collections.singletonList("%"), "my%", "name like 'my*'"},
				{Collections.singletonList("%"), "%", "name like '*'"},
				{Collections.singletonList("atsd%"), "%", "name like 'atsd*'"},
				{Collections.singletonList("atsd*"), "%", "name like 'atsd\\*'"},
				{Collections.singletonList("%"), null, "name like '*'"},
				{Collections.singletonList("atsd%"), null, "name like 'atsd*'"},
				{Collections.emptyList(), null, null},
				{Collections.emptyList(), "%", null},
				{Collections.emptyList(), "atsd%", "name like 'atsd*'"},
				{Collections.emptyList(), "my.metric", "name like 'my.metric'"},
				{Collections.emptyList(), "my_metric", "name like 'my?metric'"},
				{Collections.emptyList(), "my\\_metric", "name like 'my_metric'"},
				{Collections.emptyList(), "my%", "name like 'my*'"},
				{Collections.singletonList("abc"), null, "name like 'abc'"},
				{Collections.singletonList("abc"), "%", "name like 'abc'"},
				{Collections.singletonList("abc"), "", "name like 'abc'"},
				{Collections.singletonList("abc"), "cde", "name like 'cde'"}
		});
	}

	@Test
	@SneakyThrows(UnsupportedEncodingException.class)
	public void testExpression() {
		final String url = AtsdMeta.prepareUrlWithMetricExpression(METRICS_ENDPOINT, patternsFromConnectionString, tablesPattern);
		final String expression = url == null ? null : URLDecoder.decode(url.substring(INDEX_OF_EXPRESSION), DriverConstants.DEFAULT_CHARSET.name());
		assertThat(expression, is(expectedExpression));
	}



}