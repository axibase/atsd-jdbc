package com.axibase.tsd.driver.jdbc.ext;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class PreparedStatementsParsingTest {

	@Test
	public void splitQueryWithoutPlaceholders() {
		String query = "SELECT * FROM cpu_busy LIMIT 10 ORDER BY time DESC";
		final List<String> queryParts = AtsdMeta.splitQueryByPlaceholder(query);
		assertThat(queryParts, is(Collections.singletonList(query)));
	}

	@Test
	public void splitQueryWithPlaceholders() {
		String query = "INSERT INTO my_metric (entity, value, time) VALUES (?,?,?)";
		final List<String> expected = Arrays.asList(
				"INSERT INTO my_metric (entity, value, time) VALUES (", ",", ",", ")");
		final List<String> queryParts = AtsdMeta.splitQueryByPlaceholder(query);
		assertThat(queryParts, is(expected));
	}

	@Test
	public void splitQueryEndingWithPlaceholder() {
		String query = "SELECT * FROM jvm_memory_used WHERE entity = ? AND datetime < ?";
		final List<String> expected = Arrays.asList(
				"SELECT * FROM jvm_memory_used WHERE entity = ", " AND datetime < ", "");
		final List<String> queryParts = AtsdMeta.splitQueryByPlaceholder(query);
		assertThat(queryParts, is(expected));
	}

	@Test
	public void splitQueryWithQuestionMarkInStringLiteral() {
		String query = "SELECT 'valid?' AS \"'column' valid?\" ";
		final List<String> queryParts = AtsdMeta.splitQueryByPlaceholder(query);
		assertThat(queryParts, is(Collections.singletonList(query)));
	}
}