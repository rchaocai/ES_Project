package com.xishuang.es;

import com.xishuang.es.sql.SqlParser;

public class SqlTest {

    public static void main(String[] args) {
        String sql = "select count(),app_id from my_table where date = '2020-01-01' and time = '2'";
        SqlParser.parse(sql);
    }

}
