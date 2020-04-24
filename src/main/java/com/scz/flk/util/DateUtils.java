package com.scz.flk.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by shen on 2019/12/24.
 */
public class DateUtils {

    private static final SimpleDateFormat DT_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final SimpleDateFormat MINU_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    public static String format(Date date) {
        return DT_FORMAT.format(date);
    }

    public static String formatWithMinute(Date date) {
        return MINU_FORMAT.format(date);
    }

    public static long parse(String date) {
        try {
            return DT_FORMAT.parse(date).getTime();
        } catch (ParseException e) {
            System.err.println(date + " parse error: " + e.getMessage());
        }
        return 0L;
    }

}
