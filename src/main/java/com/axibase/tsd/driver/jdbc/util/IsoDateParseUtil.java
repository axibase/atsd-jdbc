package com.axibase.tsd.driver.jdbc.util;

import lombok.experimental.UtilityClass;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

@UtilityClass
public class IsoDateParseUtil {
    private static final ThreadLocal<Calendar> CALENDAR_CACHE = new ThreadLocal<Calendar>() {
        @Override
        protected Calendar initialValue() {
            Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
            calendar.setLenient(false);
            return calendar;
        }
    };

    public static long parseIso8601(String date) {
        try {
            int offset = 0;

            final int year = parseInt(date, offset, offset += 4);
            checkOffset(date, offset, '-');

            final int month = parseInt(date, offset += 1, offset += 2);
            checkOffset(date, offset, '-');

            final int day = parseInt(date, offset += 1, offset += 2);
            checkOffset(date, offset, 'T');

            final int hour = parseInt(date, offset += 1, offset += 2);
            checkOffset(date, offset, ':');

            final int minutes = parseInt(date, offset += 1, offset += 2);
            checkOffset(date, offset, ':');

            final int seconds = parseInt(date, offset += 1, offset += 2);

            // milliseconds can be optional in the format
            final int milliseconds;
            if (date.charAt(offset) == '.') {
                checkOffset(date, offset, '.');
                milliseconds = parseInt(date, offset += 1, offset += 3);
            } else {
                milliseconds = 0;
            }

            // extract timezone
            final char timezoneIndicator = date.charAt(offset);
            if (timezoneIndicator == 'Z') {
                if (date.length() > offset + 1) {
                    throw new IndexOutOfBoundsException("Invalid time zone indicator " + date.substring(offset));
                }
            } else {
                throw new IndexOutOfBoundsException("Invalid time zone indicator " + timezoneIndicator);
            }

            Calendar calendar = CALENDAR_CACHE.get();
            calendar.set(Calendar.YEAR, year);
            calendar.set(Calendar.MONTH, month - 1);
            calendar.set(Calendar.DAY_OF_MONTH, day);
            calendar.set(Calendar.HOUR_OF_DAY, hour);
            calendar.set(Calendar.MINUTE, minutes);
            calendar.set(Calendar.SECOND, seconds);
            calendar.set(Calendar.MILLISECOND, milliseconds);
            return calendar.getTime().getTime();
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException("Failed to parse date " + date, e);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid date " + date, e);
        }
    }

    private static int parseInt(String value, int beginIndex, int endIndex) throws NumberFormatException {
        if (beginIndex < 0 || endIndex > value.length() || beginIndex > endIndex) {
            throw new NumberFormatException(value);
        }
        // use same logic as in Integer.parseInt() but less generic we're not supporting negative values
        int i = beginIndex;
        int result = 0;
        int digit;
        if (i < endIndex) {
            digit = Character.digit(value.charAt(i++), 10);
            if (digit < 0) {
                throw new NumberFormatException("Invalid number: " + value);
            }
            result = -digit;
        }
        while (i < endIndex) {
            digit = Character.digit(value.charAt(i++), 10);
            if (digit < 0) {
                throw new NumberFormatException("Invalid number: " + value);
            }
            result *= 10;
            result -= digit;
        }
        return -result;
    }

    private static void checkOffset(String value, int offset, char expected) throws IndexOutOfBoundsException {
        char found = value.charAt(offset);
        if (found != expected) {
            throw new IndexOutOfBoundsException("Expected '" + expected + "' character but found '" + found + "'");
        }
    }
}
