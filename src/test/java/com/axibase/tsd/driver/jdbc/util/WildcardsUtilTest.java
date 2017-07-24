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
	}

	@Test
	public void atsdWildcardMatch() throws Exception {
		assertThat(WildcardsUtil.atsdWildcardMatch("jvm_memory_used",null), is(true));
		assertThat(WildcardsUtil.atsdWildcardMatch("df.disk_used","?*used*"), is(true));
		assertThat(WildcardsUtil.atsdWildcardMatch("atsd_series","atsd?series"), is(true));
		assertThat(WildcardsUtil.atsdWildcardMatch("disk_used","?isk?*"), is(true));
		assertThat(WildcardsUtil.atsdWildcardMatch("disk_used","?sk?*"), is(false));
		assertThat(WildcardsUtil.atsdWildcardMatch("disabled_entity_received_per_second","*t"), is(false));
	}

	@Test
	public void testSqlToAtsdWildcardsConvertion() {
		assertThat(WildcardsUtil.replaceSqlWildcardsWithAtsd("%t"), is("*t"));
		assertThat(WildcardsUtil.replaceSqlWildcardsWithAtsd("%_t"), is("*?t"));
		assertThat(WildcardsUtil.replaceSqlWildcardsWithAtsd("%_%%%__%t"), is("*?***??*t"));
		assertThat(WildcardsUtil.replaceSqlWildcardsWithAtsd("__t__"), is("??t??"));

	}

}