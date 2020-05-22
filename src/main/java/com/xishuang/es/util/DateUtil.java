package com.xishuang.es.util;

import com.xishuang.es.constant.DateFormatCon;

public class DateUtil {

    /**
     * 毫秒字符串转日期字符串
     */
    public static String millStr2DateStr(String millStr) {
        long mills = Long.parseLong(millStr);
        return DateFormatCon.YYYY_MM_DD_HH_MM_SS().format(mills);
    }

}
