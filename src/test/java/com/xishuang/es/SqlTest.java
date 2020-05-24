package com.xishuang.es;

import com.xishuang.es.dao.EsDao;
import com.xishuang.es.sql.SqlParser;
import com.xishuang.es.util.HttpUtil;

public class SqlTest {

    public static void main(String[] args) throws Exception {
        EsDao esDao = new EsDao(HttpUtil.getHttpHost("47.107.187.164:9200"));
//        String sql = "select count(),app_id from my_table where date = '2020-01-01' and time = '2'";
//        String sql = "select * from kibana_sample_data_logs order by bytes desc limit 5, 10";
        String sql = "select bytes,clientip from kibana_sample_data_logs where date = '2020-01-01' and b = '123' and c in ('a')  order by bytes desc limit 5, 10";
//        String sql = "select bytes,clientip from kibana_sample_data_logs where date = '2020-01-01' order by bytes desc limit 5, 10";
        SqlParser.parse(esDao, sql);
    }

}
