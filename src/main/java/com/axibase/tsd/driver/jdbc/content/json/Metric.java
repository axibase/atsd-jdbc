package com.axibase.tsd.driver.jdbc.content.json;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.calcite.avatica.com.fasterxml.jackson.annotation.JsonProperty;

@Getter
@Setter
@ToString
public class Metric{
	@JsonProperty("timePrecision")
	private String timePrecision;
	@JsonProperty("invalidAction")
	private String invalidAction;
	@JsonProperty("retentionDays")
	private int retentionDays;
	@JsonProperty("versioned")
	private boolean versioned;
	@JsonProperty("dataType")
	private String dataType;
	@JsonProperty("name")
	private String name;
	@JsonProperty("lastInsertDate")
	private String lastInsertDate;
	@JsonProperty("interpolate")
	private String interpolate;
	@JsonProperty("counter")
	private boolean counter;
	@JsonProperty("persistent")
	private boolean persistent;
	@JsonProperty("enabled")
	private boolean enabled;
}