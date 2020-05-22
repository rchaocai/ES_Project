package com.xishuang.es.constant;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * 日期格式常量
 */
public class DateFormatCon {
    /**
     * yyyy-MM-dd HH:mm:ss
     */
    public final static String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";

    /**
     * HH:mm:ss
     */
    public final static String HH_MM_SS = "HH:mm:ss";

    /**
     * HH:mm
     */
    public final static String HH_MM = "HH:mm";

    /**
     * yyyyMMdd
     */
    public final static String YYYYMMDD = "yyyyMMdd";

    /**
     * yyyy-MM-dd
     */
    public final static String YYYY_MM_DD = "yyyy-MM-dd";

    public static DateFormat YYYY_MM_DD_HH_MM_SS() {
        return new SimpleDateFormat(YYYY_MM_DD_HH_MM_SS);
    }

    public static DateFormat HH_MM_SS() {
        return new SimpleDateFormat(HH_MM_SS);
    }

    public static DateFormat HH_MM() {
        return new SimpleDateFormat(HH_MM);
    }

    public static DateFormat YYYYMMDD() {
        return new SimpleDateFormat(YYYYMMDD);
    }

    public static DateFormat YYYY_MM_DD() {
        return new SimpleDateFormat(YYYY_MM_DD);
    }
}
