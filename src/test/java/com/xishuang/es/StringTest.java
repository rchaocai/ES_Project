package com.xishuang.es;

import com.xishuang.es.util.SqlUtil;

import java.util.ArrayList;
import java.util.List;

public class StringTest {
    public static void main(String[] args) {
        List<String> list = new ArrayList<>();
        list.add("host");
        list.add("response");

        System.out.println(SqlUtil.combineGroupBy2(list, "$"));
    }
}
