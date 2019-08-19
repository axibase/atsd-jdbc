package com.axibase.tsd.driver.jdbc.enums;

import lombok.Getter;
import org.apache.calcite.avatica.Meta;


public enum SqlStatementType {
    SELECT(Meta.StatementType.SELECT) {
        @Override
        public Meta.CursorFactory getCursorFactory() {
            return Meta.CursorFactory.LIST;
        }
    },
    UPDATE(Meta.StatementType.UPDATE),
    INSERT(Meta.StatementType.INSERT),
    DELETE(Meta.StatementType.DELETE);

    @Getter
    private final Meta.StatementType avaticaStatementType;

    SqlStatementType(Meta.StatementType avaticaStatementType) {
        this.avaticaStatementType = avaticaStatementType;
    }

    public static SqlStatementType ofAvaticaStatement(Meta.StatementType type) {
        switch(type) {
            case SELECT: return SELECT;
            case UPDATE: return UPDATE;
            case INSERT: return INSERT;
            case DELETE: return DELETE;
            default: throw new IllegalArgumentException("Statement type " + type + " is not supported");
        }
    }

    public Meta.CursorFactory getCursorFactory() {
        return null;
    }
}
