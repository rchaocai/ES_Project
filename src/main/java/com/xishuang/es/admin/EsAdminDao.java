package com.xishuang.es.admin;

import com.xishuang.es.admin.domian.Index;
import com.xishuang.es.server.EsClientSingleton;
import com.xishuang.es.util.DateUtil;
import org.apache.http.HttpHost;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class EsAdminDao {
    private static final Logger LOGGER = LoggerFactory.getLogger(EsAdminDao.class);
    private IndicesClient indicesClient;

    public EsAdminDao(HttpHost... hosts) {
        LOGGER.info("init [{}]", this.getClass().getName());
        RestHighLevelClient client = EsClientSingleton.getInstance(hosts);
        indicesClient = client.indices();
    }

    public String getIndexConfig(String indexName) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
        GetIndexResponse response = this.indicesClient.get(getIndexRequest, RequestOptions.DEFAULT);
        // 解析出index信息
        Index index = new Index();

        Settings settings = response.getSettings().get(indexName);
        MappingMetaData mappings = response.getMappings().get(indexName);
        for (String key : settings.keySet()) {
            String value = settings.get(key);
            if ("index.number_of_replicas".equals(key)) {
                index.setNumber_of_replicas(value);
            } else if ("index.creation_date".equals(key)) {
                index.setCreate_date(DateUtil.millStr2DateStr(value));
            } else if ("index.number_of_shards".equals(key)) {
                index.setNumber_of_shards(value);
            } else if ("index.provided_name".equals(key)) {
                index.setIndex_name(value);
            }
        }

        System.out.println(mappings.toString());
        System.out.println(index);
        return "index";
    }


}
