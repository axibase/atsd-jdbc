package com.axibase.tsd.driver.jdbc.util;

import lombok.experimental.UtilityClass;

import java.math.BigDecimal;
import java.math.MathContext;

@UtilityClass
public class NumberParseUtil {
    public static long parseLongFast(String value) {
        if (value == null) {
            throw new NumberFormatException("null");
        }
        int length = value.length();
        return parseLongFast(value, length, 0, length);
    }

    public static long parseLongFast(String value, int start, int end) {
        if (value == null) {
            throw new NumberFormatException("null");
        }
        int length = value.length();
        if (start < 0  || end > length || start > end) {
            throw new ArrayIndexOutOfBoundsException();
        }
        return parseLongFast(value, end - start, start, end);
    }

    private static long parseLongFast(String value, int len, int start, int end) {
        if (len == 0) {
            throw new NumberFormatException("Empty string could not be parsed as long");
        }
        int i = start;
        long result = 0;
        boolean negative = false;
        long limit = -Long.MAX_VALUE;
        int digit;
        char ch;
        char firstChar = value.charAt(0);
        if (firstChar < '0') { // Possible leading "+" or "-"
            if (firstChar == '-') {
                negative = true;
                limit = Long.MIN_VALUE;
            } else if (firstChar != '+') {
                throw new NumberFormatException(value + " could not be parsed as long");
            }

            if (len == 1) // Cannot have lone "+" or "-"
                throw new NumberFormatException(value + " could not be parsed as long");
            i++;
        }
        long multmin = limit / 10;
        while (i < end) {
            // Accumulating negatively avoids surprises near MAX_VALUE
            ch = value.charAt(i++);
            digit = ch - '0';
            if (digit < 0 || digit > 9) {
                if (ch == '.' && isFractionalPartZero(value, i + 1, end)) {
                    break;
                }
                // If floating-point, scientific, or non-ascii numbers used, fallback to BigDecimal
                try {
                    return new BigDecimal(value.toCharArray(), start, len, MathContext.UNLIMITED).longValueExact();
                } catch (ArithmeticException e) {
                    throw new NumberFormatException("Fractional number " + value + " could not be parsed as long");
                }
            }
            if (result < multmin) {
                throw new NumberFormatException(value + " could not be parsed as long");
            }
            result *= 10;
            if (result < limit + digit) {
                throw new NumberFormatException(value + " could not be parsed as long");
            }
            result -= digit;
        }
        return negative ? result : -result;
    }

    private static boolean isFractionalPartZero(String value, int start, int end) {
        for (int i = start; i < end; i++) {
            if (value.charAt(i) != '0') {
                return false;
            }
        }
        return true;
    }
}
