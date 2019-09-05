package com.axibase.tsd.driver.jdbc.util;

import com.axibase.tsd.driver.jdbc.enums.SqlStatementType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class StatementTypeByQueryRecognizerTest {
	private final String sql;
	private final SqlStatementType expectedStatementType;

	public StatementTypeByQueryRecognizerTest(String sql, SqlStatementType expectedStatementType) {
		this.sql = sql;
		this.expectedStatementType = expectedStatementType;
	}

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
				{"SELECT * FROM jvm_memory_used", SqlStatementType.SELECT},
				{"SELECT\n *\n  FROM jvm_memory_used", SqlStatementType.SELECT},
				{"SELECT\t*\tFROM\tjvm_memory_used", SqlStatementType.SELECT},
				{"SELECT * FROM jvm_memory_used", SqlStatementType.SELECT},
				{"INSERT INTO test_table (entity, datetime, value) VALUES ('test_entity', '2017-01-01T00:00:00Z', 42.0)", SqlStatementType.INSERT},
				{"INSERT\nINTO\ntest_table\n(entity, datetime, value)\nVALUES\n('test_entity', '2017-01-01T00:00:00Z', 42.0)", SqlStatementType.INSERT},
				{"INSERT\tINTO\ttest_table\t(entity, datetime, value)\nVALUES\n('test_entity', '2017-01-01T00:00:00Z', 42.0)", SqlStatementType.INSERT},
				{" INSERT INTO test_table (entity, datetime, value) VALUES ('test_entity', '2017-01-01T00:00:00Z', 42.0)", SqlStatementType.INSERT},
				{"INSERT INTO \"test_table\" (entity, datetime, value) VALUES ('test_entity', '2017-01-01T00:00:00Z', 42.0)", SqlStatementType.INSERT},
				{"UPDATE test_table SET value = -1 WHERE entity ='test_entity' AND datetime = '2017-01-01T00:00:00Z'", SqlStatementType.UPDATE},
				{"UPDATE\ntest_table\nSET value = -1\nWHERE entity ='test_entity' AND datetime = '2017-01-01T00:00:00Z'", SqlStatementType.UPDATE},
				{"UPDATE\ttest_table\tSET value = -1\tWHERE entity ='test_entity' AND datetime = '2017-01-01T00:00:00Z'", SqlStatementType.UPDATE},
				{" UPDATE test_table SET value = -1 WHERE entity ='test_entity' AND datetime = '2017-01-01T00:00:00Z'", SqlStatementType.UPDATE},
				{"UPDATE \"test_table\" SET value = -1 WHERE entity ='test_entity' AND datetime = '2017-01-01T00:00:00Z'", SqlStatementType.UPDATE},
		});
	}

	@Test
	public void testRecognition() throws Exception {
		assertThat(EnumUtil.getStatementTypeByQuery(sql), is(expectedStatementType));
	}

}