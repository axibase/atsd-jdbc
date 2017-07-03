package com.axibase.tsd.driver.jdbc.converter;

import com.axibase.tsd.driver.jdbc.util.CaseInsensitiveLinkedHashMap;
import java.util.Map;

class SeriesCommand extends AbstractCommand {

    private final Map<String, Double> numericValues = new CaseInsensitiveLinkedHashMap<>();
    private final Map<String, String> textValues = new CaseInsensitiveLinkedHashMap<>();

    SeriesCommand() {
        super("series");
    }

    public void addValue(String name, Double value) {
        if (name == null || value == null) {
            return;
        }
        numericValues.put(name.toLowerCase(), value);
    }

    public void addValue(String name, String value) {
        if (name == null || value == null) {
            return;
        }
        textValues.put(name.toLowerCase(), value);
    }

    @Override
    protected void validate() {
        super.validate();
        if (numericValues.isEmpty() && textValues.isEmpty()) {
            throw new IllegalArgumentException("Numeric and text values not defined");
        }
    }

    @Override
    protected void appendValues(StringBuilder buffer) {
        for (Map.Entry<String, Double> entry : numericValues.entrySet()){
            buffer.append(" m:").append(handleName(entry.getKey()))
                    .append('=').append(formatMetricValue(entry.getValue()));
        }

        appendKeysAndValues(buffer, " x:", textValues);
    }

    private static String formatMetricValue(double value) {
        if (Double.isNaN(value)) {
            return "NaN";
        }
        return Double.toString(value);
    }

    @Override
    public String toString() {
        return "SeriesCommand{" + super.toString() +
                ", numericValues=" + numericValues +
                ", textValues=" + textValues +
                '}';
    }

}
