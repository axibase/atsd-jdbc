package com.axibase.tsd.driver.jdbc.strategies;

import com.univocity.parsers.csv.CsvParserSettings;

public class Trojan {
    public static CsvParserSettings preparesSettings() {
        return RowIterator.prepareParserSettings(null);
    }
}
