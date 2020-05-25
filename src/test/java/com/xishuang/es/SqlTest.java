package com.xishuang.es;

import com.xishuang.es.dao.EsDao;
import com.xishuang.es.sql.SqlParser;
import com.xishuang.es.util.HttpUtil;

public class SqlTest {

    public static void main(String[] args) throws Exception {
        EsDao esDao = new EsDao(HttpUtil.getHttpHost("47.107.187.164:9200"));
//        String sql = "select count(),app_id from my_table where date = '2020-01-01' and time = '2'";
//        String sql = "select * from kibana_sample_data_logs order by bytes desc limit 5, 10";
//        String sql = "select bytes,clientip from kibana_sample_data_logs where date = '2020-01-01' and b = '123' and c in ('a', 'haha')  order by bytes desc limit 5, 10";
//        String sql = "select bytes,clientip from kibana_sample_data_logs where clientip = '248.150.117.171' and extension in ('deb') order by bytes desc limit 5, 10";
//        String sql = "select bytes,clientip from kibana_sample_data_logs where clientip = '248.150.117.171' order by bytes desc limit 5, 10";
//        String sql = "select * from kibana_sample_data_logs where clientip = '248.150.117.171' and host in ('www.elastic.co') order by bytes desc limit 1, 10";
//        String sql = "select * from kibana_sample_data_logs where host in ('www.elastic.co') and bytes >= 3000 and bytes <= 4000 and response = 200 or response = 300 order by bytes desc limit 1, 10";
//        String sql = "select * from kibana_sample_data_logs where host in ('www.elastic.co') and bytes not between 3000 and 4000 and response = 200 or response = 300 order by bytes desc limit 1, 10";
//        String sql = "select * from kibana_sample_data_logs where host in ('www.elastic.co') order by bytes desc limit 1, 10";
//        String sql = "select host.keyword, count() from kibana_sample_data_logs group by host,response having a > 3";
        String sql = "select host.keyword, count() from kibana_sample_data_logs group by host having a > 2808";
        SqlParser.parse(esDao, sql);
    }

}
