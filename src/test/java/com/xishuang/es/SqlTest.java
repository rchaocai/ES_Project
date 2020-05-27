package com.xishuang.es;

import com.xishuang.es.dao.EsDao;
import com.xishuang.es.domain.Result;
import com.xishuang.es.sql.SqlParser;
import com.xishuang.es.util.HttpUtil;

public class SqlTest {

    public static void main(String[] args) throws Exception {
        EsDao esDao = new EsDao(HttpUtil.getHttpHost("47.107.187.164:9200"));
        Result result = new Result<>();
        result.setDebug(true);

//        String sql = "select count(),app_id from my_table where date = '2020-01-01' and time = '2'";
//        String sql = "select * from kibana_sample_data_logs order by bytes desc limit 5, 10";
//        String sql = "select bytes,clientip from kibana_sample_data_logs where date = '2020-01-01' and b = '123' and c in ('a', 'haha')  order by bytes desc limit 5, 10";
//        String sql = "select bytes,clientip from kibana_sample_data_logs where clientip = '248.150.117.171' and extension in ('deb') order by bytes desc limit 5, 10";
//        String sql = "select bytes,clientip from kibana_sample_data_logs where clientip = '248.150.117.171' order by bytes desc limit 5, 10";
//        String sql = "select * from kibana_sample_data_logs where clientip = '248.150.117.171' and host in ('www.elastic.co') order by bytes limit 1, 10";
//        String sql = "select * from kibana_sample_data_logs where host in ('www.elastic.co') and bytes >= 3000 and bytes <= 4000 and response = 200 or response = 300 order by bytes desc limit 1, 10";
//        String sql = "select * from kibana_sample_data_logs where host in ('www.elastic.co') and bytes not between 3000 and 4000 and response = 200 or response = 300 order by bytes desc limit 1, 10";
//        String sql = "select * from kibana_sample_data_logs where host in ('www.elastic.co') order by bytes desc limit 1, 10";
//        String sql = "select host.keyword, count() from kibana_sample_data_logs group by host,response having a > 3";
//        String sql = "select host.keyword, count(), sum(bytes) as haha from kibana_sample_data_logs group by host having haha > 280 limit 5";
//        String sql = "select host.keyword, count() as count, sum(bytes) from kibana_sample_data_logs group by host,response having count > 280 order by count,host desc limit 5";
        String sql = "select host from (select host,bytes,response,row_number()over(partition by host.keyword,response order by bytes desc) rank from kibana_sample_data_logs where bytes between 3000 and 4000 limit 3) where rank <1";
//        String sql = "select host.keyword, DateInterval(utc_time, INTERVAL 1 day) as count from kibana_sample_data_logs where utc_time >= '2020-07-01' group by count having count > 300 order by count";
//        String sql = "select host.keyword, DateInterval(utc_time, INTERVAL 2 day) as utc_time_internal, count()  from kibana_sample_data_logs group by utc_time_internal order by utc_time_internal desc";

        SqlParser.parse(esDao, sql, result);
    }
}
