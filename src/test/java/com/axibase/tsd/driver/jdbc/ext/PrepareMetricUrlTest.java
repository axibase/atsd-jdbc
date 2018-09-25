package com.axibase.tsd.driver.jdbc.ext;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@RunWith(Parameterized.class)
public class PrepareMetricUrlTest {
	private final List<String> patternsFromConnectionString;
	private final String tablesPattern;
	private final Object expectedExpression;

	public PrepareMetricUrlTest(List<String> patternsFromConnectionString, String tablesPattern, Object expectedExpression) {
		this.tablesPattern = tablesPattern;
		this.patternsFromConnectionString = patternsFromConnectionString;
		this.expectedExpression = expectedExpression instanceof Collection ?expectedExpression : Collections.singletonList(expectedExpression);
	}

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
				{Collections.singletonList("%"), "my.metric", "my.metric"},
				{Collections.singletonList("%"), "my_metric", "name like 'my?metric'"},
				{Collections.singletonList("%"), "my%", "name like 'my*'"},
				{Collections.singletonList("%"), "%", "name like '*'"},
				{Collections.singletonList("atsd%"), "%", "name like 'atsd*'"},
				{Collections.singletonList("atsd*"), "%", "name like 'atsd\\*'"},
				{Collections.singletonList("%"), null, "name like '*'"},
				{Collections.singletonList("atsd%"), null, "name like 'atsd*'"},
				{Collections.emptyList(), null, Collections.emptyList()},
				{Collections.emptyList(), "%", Collections.emptyList()},
				{Collections.emptyList(), "atsd%", "name like 'atsd*'"},
				{Collections.emptyList(), "my.metric", "my.metric"},
				{Collections.emptyList(), "my_metric", "name like 'my?metric'"},
				{Collections.emptyList(), "my\\_metric", "my_metric"},
				{Collections.emptyList(), "my%", "name like 'my*'"},
				{Collections.singletonList("abc"), null, "abc"},
				{Collections.singletonList("abc"), "%", "abc"},
				{Collections.singletonList("abc"), "", "abc"},
				{Collections.singletonList("abc"), "cde", "cde"},
				{Arrays.asList("abc","cde"), "cde", "cde"},
				{Arrays.asList("abc","cde"), null, Arrays.asList("abc","cde")},
		});
	}

	@Test
	public void testExpression() {
		final Collection<AtsdMeta.MetricLocation> metricLocations = AtsdMeta.prepareGetMetricUrls(patternsFromConnectionString, tablesPattern, false);
		final Collection<String> metricExpressions = new ArrayList<>(metricLocations.size());
		for (AtsdMeta.MetricLocation metricLocation : metricLocations) {
			metricExpressions.add(metricLocation.getExpression());
		}
		assertThat(metricExpressions, is(expectedExpression));
	}



}