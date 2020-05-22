package com.xishuang.es.dao;

import com.xishuang.es.EsClientSingleton;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestHighLevelClient;

public class EsDao {
    private static final Logger LOGGER = LogManager.getLogger(EsDao.class);
    protected RestHighLevelClient client;

    public EsDao(HttpHost... hosts) {
        LOGGER.info("init [{}]", this.getClass().getName());
        this.client = EsClientSingleton.getInstance(hosts);
    }


}
