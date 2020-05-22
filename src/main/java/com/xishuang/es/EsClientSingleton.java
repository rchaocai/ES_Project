package com.xishuang.es;

import com.xishuang.es.util.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 单例创建client
 */
public class EsClientSingleton {
    private static final Logger LOGGER = LoggerFactory.getLogger(EsClientSingleton.class);
    private static volatile RestHighLevelClient client;

    private EsClientSingleton() {}

    public static RestHighLevelClient getInstance(HttpHost... hosts) {
        if (client == null) {
            synchronized (EsClientSingleton.class) {
                if (client == null) {
                    LOGGER.info("创建elasticsearch连接[{}]", StringUtils.toString(hosts));
                    RestClientBuilder builder = RestClient.builder(hosts).setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                        @Override
                        public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                            requestConfigBuilder.setConnectTimeout(5000);
                            requestConfigBuilder.setSocketTimeout(5 * 60 * 1000); // 5分钟
                            requestConfigBuilder.setConnectionRequestTimeout(1000);
                            return requestConfigBuilder;
                        }
                    });
                    client = new RestHighLevelClient(builder);
                }
            }
        }
        return client;
    }
}
