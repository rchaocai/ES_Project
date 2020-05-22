package com.xishuang.es.util;

import org.apache.http.HttpHost;

public class HttpUtil {

    public static HttpHost[] getHttpHost(String ipPort) {
        String[] esHosts = ipPort.split(",");
        HttpHost[] httpHosts = new HttpHost[esHosts.length];
        for (int i = 0; i < esHosts.length; i++) {
            String esHost = esHosts[i];
            String[] tmps = esHost.split(":");
            httpHosts[i] = new HttpHost(tmps[0], Integer.parseInt(tmps[1]));
        }

        return httpHosts;
    }

}
