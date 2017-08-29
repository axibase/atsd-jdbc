package com.axibase.tsd.driver.jdbc.ext;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class NormalizeQueryTest {
    private final String sql;
    private final String expectedNormalizedQuery;

    public NormalizeQueryTest(String sql, String expectedNormalizedQuery) {
        this.sql = sql;
        this.expectedNormalizedQuery = expectedNormalizedQuery;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"SELECT * FROM jvm_memory_used", "SELECT * FROM jvm_memory_used"},
                {"  \t SELECT\n *\n  FROM jvm_memory_used", "SELECT\n *\n  FROM jvm_memory_used"},
                {"INSERT INTO test_table (entity, datetime, value) VALUES ('test_entity', '2017-01-01T00:00:00Z', 42.0)",
                        "INSERT INTO test_table (entity, datetime, value) VALUES ('test_entity', '2017-01-01T00:00:00Z', 42.0)"},
                {"  \t  INSERT\nINTO\ntest_table\n(entity, datetime, value)\nVALUES\n('test_entity', '2017-01-01T00:00:00Z', 42.0)",
                        "INSERT\nINTO\ntest_table\n(entity, datetime, value)\nVALUES\n('test_entity', '2017-01-01T00:00:00Z', 42.0)"},
                {"--comment \n SELECT * FROM jvm_memory_used", "SELECT * FROM jvm_memory_used"},
                {"--comment \n --comment \n --comment  \n  SELECT * FROM jvm_memory_used", "SELECT * FROM jvm_memory_used"},
                {"--comment \n --comment \n --comment  \n  ", null},
        });
    }

    @Test
    public void testRecognition() throws Exception {
        assertThat(AtsdMeta.normalizeQuery(sql), is(expectedNormalizedQuery));
    }

}