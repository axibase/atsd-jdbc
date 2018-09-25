package com.axibase.tsd.driver.jdbc.util;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class WildcardsUtilTest {
	@Test
	public void wildcardMatch() throws Exception {
		assertThat(WildcardsUtil.wildcardMatch("jvm_memory_used",null), is(true));
		assertThat(WildcardsUtil.wildcardMatch("df.disk_used","_%used%"), is(true));
		assertThat(WildcardsUtil.wildcardMatch("atsd_series","atsd_series"), is(true));
		assertThat(WildcardsUtil.wildcardMatch("disk_used","_isk_%"), is(true));
		assertThat(WildcardsUtil.wildcardMatch("disk_used","_sk_%"), is(false));
		assertThat(WildcardsUtil.wildcardMatch("disabled_entity_received_per_second","%t"), is(false));
		assertThat(WildcardsUtil.wildcardMatch("atsd_series","atsd\\_series"), is(true));
	}

	@Test
	public void hasWildcards() throws Exception {
		assertThat(WildcardsUtil.hasWildcards("jvm_memory_used"), is(true));
		assertThat(WildcardsUtil.hasWildcards("jvm\\_memory\\_used"), is(false));
		assertThat(WildcardsUtil.hasWildcards("jvm\\_memory_used"), is(true));
	}

	@Test
	public void testSqlToAtsdWildcardsConvertionWithEscapeUnderscoreAsLiteral() {
		final boolean underscoreAsLiteral = true;
		assertThat(WildcardsUtil.replaceSqlWildcardsWithAtsdUseEscaping("%t", underscoreAsLiteral), is("*t"));
		assertThat(WildcardsUtil.replaceSqlWildcardsWithAtsdUseEscaping("%\\t", underscoreAsLiteral), is("*\\t"));
		assertThat(WildcardsUtil.replaceSqlWildcardsWithAtsdUseEscaping("%_t", underscoreAsLiteral), is("*_t"));
		assertThat(WildcardsUtil.replaceSqlWildcardsWithAtsdUseEscaping("%\\_t", underscoreAsLiteral), is("*\\_t"));
		assertThat(WildcardsUtil.replaceSqlWildcardsWithAtsdUseEscaping("%_**%__%t", underscoreAsLiteral), is("*_\\*\\**__*t"));
		assertThat(WildcardsUtil.replaceSqlWildcardsWithAtsdUseEscaping("%_%%%__%t*", underscoreAsLiteral), is("*_***__*t\\*"));
		assertThat(WildcardsUtil.replaceSqlWildcardsWithAtsdUseEscaping("????", underscoreAsLiteral), is("\\?\\?\\?\\?"));
	}

	@Test
	public void testSqlToAtsdWildcardsConvertionWithEscapeUnderscoreAsMetacharacter() {
		final boolean underscoreAsLiteral = false;
		assertThat(WildcardsUtil.replaceSqlWildcardsWithAtsdUseEscaping("%t", underscoreAsLiteral), is("*t"));
		assertThat(WildcardsUtil.replaceSqlWildcardsWithAtsdUseEscaping("%\\t", underscoreAsLiteral), is("*\\t"));
		assertThat(WildcardsUtil.replaceSqlWildcardsWithAtsdUseEscaping("%_t", underscoreAsLiteral), is("*?t"));
		assertThat(WildcardsUtil.replaceSqlWildcardsWithAtsdUseEscaping("%\\_t", underscoreAsLiteral), is("*_t"));
		assertThat(WildcardsUtil.replaceSqlWildcardsWithAtsdUseEscaping("%_**%__%t", underscoreAsLiteral), is("*?\\*\\**??*t"));
		assertThat(WildcardsUtil.replaceSqlWildcardsWithAtsdUseEscaping("%_%%%__%t*", underscoreAsLiteral), is("*?***??*t\\*"));
		assertThat(WildcardsUtil.replaceSqlWildcardsWithAtsdUseEscaping("????", underscoreAsLiteral), is("\\?\\?\\?\\?"));
	}

}