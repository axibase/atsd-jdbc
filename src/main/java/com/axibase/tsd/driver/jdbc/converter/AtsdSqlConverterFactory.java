package com.axibase.tsd.driver.jdbc.converter;

import org.apache.calcite.avatica.Meta.StatementType;

public final class AtsdSqlConverterFactory {

    private AtsdSqlConverterFactory() {
    }

    public static AtsdSqlConverter getConverter(StatementType statementType) {
        switch (statementType) {
            case INSERT: return new AtsdSqlInsertConverter();
            case UPDATE: return new AtsdSqlUpdateConverter();
            default: throw new IllegalArgumentException("Illegal statement type: " + statementType);
        }
    }

}
