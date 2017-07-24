package com.axibase.tsd.driver.jdbc.converter;

import org.apache.calcite.avatica.Meta.StatementType;

public final class AtsdSqlConverterFactory {

    private AtsdSqlConverterFactory() {
    }

    public static AtsdSqlConverter getConverter(StatementType statementType, boolean timestampTz) {
        switch (statementType) {
            case INSERT: return new AtsdSqlInsertConverter(timestampTz);
            case UPDATE: return new AtsdSqlUpdateConverter(timestampTz);
            default: throw new IllegalArgumentException("Illegal statement type: " + statementType);
        }
    }

}
