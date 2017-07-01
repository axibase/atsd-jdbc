package com.axibase.tsd.driver.jdbc.content.json;

import org.apache.calcite.avatica.com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Generated;

@Generated("com.robohorse.robopojogenerator")
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

	public void setTimePrecision(String timePrecision){
		this.timePrecision = timePrecision;
	}

	public String getTimePrecision(){
		return timePrecision;
	}

	public void setInvalidAction(String invalidAction){
		this.invalidAction = invalidAction;
	}

	public String getInvalidAction(){
		return invalidAction;
	}

	public void setRetentionDays(int retentionDays){
		this.retentionDays = retentionDays;
	}

	public int getRetentionDays(){
		return retentionDays;
	}

	public void setVersioned(boolean versioned){
		this.versioned = versioned;
	}

	public boolean isVersioned(){
		return versioned;
	}

	public void setDataType(String dataType){
		this.dataType = dataType;
	}

	public String getDataType(){
		return dataType;
	}

	public void setName(String name){
		this.name = name;
	}

	public String getName(){
		return name;
	}

	public void setLastInsertDate(String lastInsertDate){
		this.lastInsertDate = lastInsertDate;
	}

	public String getLastInsertDate(){
		return lastInsertDate;
	}

	public void setInterpolate(String interpolate){
		this.interpolate = interpolate;
	}

	public String getInterpolate(){
		return interpolate;
	}

	public void setCounter(boolean counter){
		this.counter = counter;
	}

	public boolean isCounter(){
		return counter;
	}

	public void setPersistent(boolean persistent){
		this.persistent = persistent;
	}

	public boolean isPersistent(){
		return persistent;
	}

	public void setEnabled(boolean enabled){
		this.enabled = enabled;
	}

	public boolean isEnabled(){
		return enabled;
	}

	@Override
 	public String toString(){
		return 
			"Metric{" + 
			"timePrecision = '" + timePrecision + '\'' + 
			",invalidAction = '" + invalidAction + '\'' + 
			",retentionDays = '" + retentionDays + '\'' + 
			",versioned = '" + versioned + '\'' + 
			",dataType = '" + dataType + '\'' + 
			",name = '" + name + '\'' + 
			",lastInsertDate = '" + lastInsertDate + '\'' + 
			",interpolate = '" + interpolate + '\'' + 
			",counter = '" + counter + '\'' + 
			",persistent = '" + persistent + '\'' + 
			",enabled = '" + enabled + '\'' + 
			"}";
		}
}