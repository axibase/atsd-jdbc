package com.axibase.tsd.driver.jdbc.content.json;

import org.apache.calcite.avatica.com.fasterxml.jackson.annotation.JsonProperty;

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

    public Integer getFail() {
        return fail;
    }

    public void setFail(Integer fail) {
        this.fail = fail;
    }

    public Integer getSuccess() {
        return success;
    }

    public void setSuccess(Integer success) {
        this.success = success;
    }

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public Integer getStored() {
        return stored;
    }

    public void setStored(Integer stored) {
        this.stored = stored;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    @Override
    public String toString() {
        return "SendCommandResult " + String.format("[fail=%s, success=%s, total=%s, stored=%s, error=%s]",
                fail, success, total, stored, error
        );
    }
}
