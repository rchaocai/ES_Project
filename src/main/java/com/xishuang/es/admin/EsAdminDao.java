package com.xishuang.es.admin;

import com.alibaba.fastjson.JSONObject;
import com.xishuang.es.admin.domian.Index;
import com.xishuang.es.admin.domian.Mappings;
import com.xishuang.es.EsClientSingleton;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 针对索引的增删查
 */
public class EsAdminDao {
    private static final Logger LOGGER = LoggerFactory.getLogger(EsAdminDao.class);
    private final IndicesClient indicesClient;

    public EsAdminDao(HttpHost... hosts) {
        LOGGER.info("init [{}]", this.getClass().getName());
        RestHighLevelClient client = EsClientSingleton.getInstance(hosts);
        indicesClient = client.indices();
    }

    /**
     * 创建索引
     *
     * @param index  索引的相关设置
     * @param fields 需要添加的各个字段
     */
    public boolean createIndex(Index index, Map<String, Mappings.Property> fields) {
        LOGGER.info("创建索引 [{}]", index);
        CreateIndexRequest request = new CreateIndexRequest(index.getIndex_name());
        // 字段信息添加到mapping中
        index.getMappings().setProperties(fields);
        // 设置settings
        request.settings(index.getSettings());
        // 设置mappings
        String sourceJson = JSONObject.toJSONString(index.getMappings());
        request.mapping(sourceJson, XContentType.JSON);
        CreateIndexResponse response;
        try {
            response = this.indicesClient.create(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            LOGGER.error("index[{}]创建失败, 响应信息", index.getIndex_name(), e);
            return false;
        }
        String responseMsg = "{\"isAcknowledged\":" + response.isAcknowledged() + ", \"isShardsAcknowledged\":" + response.isShardsAcknowledged() + "}";
        LOGGER.info("index[{}]创建成功, 响应信息[{}]", index.getIndex_name(), responseMsg);
        return true;
    }

    public boolean deleteIndex(String indexName) {
        LOGGER.info("删除索引 [{}]", indexName);
        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
        AcknowledgedResponse response;
        try {
            response = this.indicesClient.delete(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            LOGGER.error("index[{}]删除失败, 响应信息", indexName, e);
            return false;
        }
        String responseMsg = "{\"isAcknowledged\":" + response.isAcknowledged() + "}";
        LOGGER.info("index[{}]删除成功, 响应信息[{}]", indexName, responseMsg);
        return true;
    }

    /**
     * 查询索引具体信息
     */
    public Index queryIndexInfo(String indexName) throws IOException {
        LOGGER.info("查询索引 [{}]", indexName);
        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
        GetIndexResponse response = this.indicesClient.get(getIndexRequest, RequestOptions.DEFAULT);

        Index index = new Index();
        index.setIndex_name(indexName);
        index.setSettings(new HashMap<>());
        // 解析出setting信息
        Settings settings = response.getSettings().get(indexName);
        for (String key : settings.keySet()) {
            String value = settings.get(key);
            index.getSettings().put(key, value);
        }
        // 解析出mapping信息
        String mappingData = JSONObject.toJSONString(response.getMappings().get(indexName).getSourceAsMap());
        System.out.println(mappingData);
        index.setMappings(JSONObject.parseObject(mappingData, Mappings.class));

        return index;
    }


}
