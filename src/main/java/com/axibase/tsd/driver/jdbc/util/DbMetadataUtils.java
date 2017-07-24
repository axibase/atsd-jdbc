package com.axibase.tsd.driver.jdbc.util;

import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;

@UtilityClass
public class DbMetadataUtils {
	private static final String[] SQL_KEYWORDS = {"ROW_NUMBER","INTERPOLATE","ROW_MEMORY_THRESHOLD",
			"OPTION","ISNULL","IF","ELSE","ELSEIF","END","LAST_TIME","USING","ENTITY",
			"PREVIOUS","NEXT","LINEAR","VALUE","START_TIME","END_TIME","FIRST_VALUE_TIME",
			"CALENDAR","LINEAR","OUTER","NAN","EXTEND","METRICS","ENDTIME","INTERVAL_NUMBER"};

	private static final String[] STRING_FUNCTIONS = {"REGEX","UPPER","LOWER","REPLACE","LENGTH","CONCAT","LOCATE","SUBSTR"};

	public static String getSqlKeywords() {
		return StringUtils.join(SQL_KEYWORDS, ',');
	}

	public static String getNumericFunctions() {
		return "CORREL,SQRT";
	}

	public static String getStringFunctions() {
		return StringUtils.join(STRING_FUNCTIONS, ',');
	}

	public static String getSupportedTimeFunctions() {
		return "DATE_PARSE,DATE_FORMAT,CURRENT_TIMESTAMP,DBTIMEZONE,PERIOD";
	}
}
