package com.xishuang.es;

import com.xishuang.es.admin.EsAdminDao;
import com.xishuang.es.admin.domian.Index;
import com.xishuang.es.admin.domian.Mappings;
import com.xishuang.es.util.HttpUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AdminTest {

    public static void main(String[] args) throws IOException {
        EsAdminDao esAdminDao = new EsAdminDao(HttpUtil.getHttpHost("47.107.187.164:9200"));
//        queryIndex(esAdminDao);
//        createIndex(esAdminDao);
        deleteIndex(esAdminDao);
    }

    private static void queryIndex(EsAdminDao esAdminDao) throws IOException {
        System.out.println(esAdminDao.queryIndexInfo("kibana_sample_data_logs"));
    }

    private static void deleteIndex(EsAdminDao esAdminDao) {
        System.out.println(esAdminDao.deleteIndex("create_index_test"));
    }

    private static void createIndex(EsAdminDao esAdminDao) {
        Index index = new Index();
        index.setSettings(new HashMap<>());
        index.setIndex_name("create_index_test");
        index.getSettings().put("index.number_of_shards", "1");
        index.getSettings().put("index.number_of_replicas", "1");

        Map<String, Mappings.Property> propertyMap = new HashMap<>();
        Mappings.Property haha = new Mappings.Property();
        haha.setType("long");
        Mappings.Property hehe = new Mappings.Property();
        hehe.setType("text");
        propertyMap.put("haha", haha);
        propertyMap.put("hehe", hehe);

        System.out.println(esAdminDao.createIndex(index, propertyMap));
    }
}
