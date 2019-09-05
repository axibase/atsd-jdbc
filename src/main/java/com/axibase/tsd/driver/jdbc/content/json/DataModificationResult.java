package com.axibase.tsd.driver.jdbc.content.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class DataModificationResult {
    @JsonProperty("fail")
    private Integer updated;

    @JsonProperty("success")
    private Integer deleted;

    public long countUpdated() {
        long total = 0L;
        if (updated != null) {
            total += updated;
        }
        if (deleted != null) {
            total += deleted;
        }
        return total;
    }
}
