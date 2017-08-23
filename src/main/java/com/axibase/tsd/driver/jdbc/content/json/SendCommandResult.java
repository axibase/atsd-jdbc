package com.axibase.tsd.driver.jdbc.content.json;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.calcite.avatica.com.fasterxml.jackson.annotation.JsonProperty;

@Getter
@Setter
@ToString
public class SendCommandResult {
    @JsonProperty("fail")
    private Integer fail;

    @JsonProperty("success")
    private Integer success;

    @JsonProperty("total")
    private Integer total;

    @JsonProperty("stored")
    private Integer stored;

    @JsonProperty("error")
    private String error;
}
