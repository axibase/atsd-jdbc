package com.axibase.tsd.driver.jdbc.ext;

import com.axibase.tsd.driver.jdbc.enums.AtsdType;
import com.axibase.tsd.driver.jdbc.enums.JsonConvertedType;
import org.apache.calcite.avatica.ColumnMetaData;

public final class AtsdColumnMetaData extends ColumnMetaData {
    public final AvaticaType exposedType;
    public final String originalColumnName;
    public final JsonConvertedType jsonConvertedType;

    public AtsdColumnMetaData(int ordinal, int nullable, String label, String columnName, String schemaName,
                              String tableName, String catalogName, AtsdType atsdType, AvaticaType internalType,
                              AvaticaType exposedType, boolean assignColumnNames, JsonConvertedType jsonConvertedType) {
        super(ordinal, false, false, false, false, nullable, false,
                atsdType.size, label, assignColumnNames ? columnName : label, schemaName, atsdType.maxPrecision,
                atsdType.scale, tableName, catalogName, internalType, true,
                false, false, internalType.rep.clazz.getCanonicalName());
        this.exposedType = exposedType;
        this.originalColumnName = columnName;
        this.jsonConvertedType = jsonConvertedType;
    }
}
