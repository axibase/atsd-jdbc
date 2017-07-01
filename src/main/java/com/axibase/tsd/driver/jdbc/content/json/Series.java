/*
* Copyright 2016 Axibase Corporation or its affiliates. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
* https://www.axibase.com/atsd/axibase-apache-2.0.pdf
*
* or in the "license" file accompanying this file. This file is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/
package com.axibase.tsd.driver.jdbc.content.json;

import org.apache.calcite.avatica.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Generated;
import java.util.Map;

@Generated("com.robohorse.robopojogenerator")
public class Series{

	@JsonProperty("metric")
	private String metric;

	@JsonProperty("lastInsertDate")
	private String lastInsertDate;

	@JsonProperty("entity")
	private String entity;

	@JsonProperty("tags")
	private Map<String, String> tags;

	public void setMetric(String metric){
		this.metric = metric;
	}

	public String getMetric(){
		return metric;
	}

	public void setLastInsertDate(String lastInsertDate){
		this.lastInsertDate = lastInsertDate;
	}

	public String getLastInsertDate(){
		return lastInsertDate;
	}

	public void setEntity(String entity){
		this.entity = entity;
	}

	public String getEntity(){
		return entity;
	}

	public void setTags(Map<String, String> tags){
		this.tags = tags;
	}

	public Map<String, String> getTags(){
		return tags;
	}

	@Override
 	public String toString(){
		return 
			"Series{" + 
			"metric = '" + metric + '\'' + 
			",lastInsertDate = '" + lastInsertDate + '\'' + 
			",entity = '" + entity + '\'' + 
			",tags = '" + tags + '\'' + 
			"}";
		}
}