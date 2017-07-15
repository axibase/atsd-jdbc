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
	}

}