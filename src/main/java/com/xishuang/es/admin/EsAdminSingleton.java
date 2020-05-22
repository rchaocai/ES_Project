package com.xishuang.es.admin;

import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 单例创建es管理类
 */
public class EsAdminSingleton {
    private static final Logger LOGGER = LoggerFactory.getLogger(EsAdminSingleton.class);
    private static volatile EsAdminDao esAdminDao;

    private EsAdminSingleton() {
    }

    public static EsAdminDao getInstance(HttpHost... hosts) {
        if (esAdminDao == null) {
            synchronized (EsAdminSingleton.class) {
                if (esAdminDao == null) {
                    esAdminDao = new EsAdminDao(hosts);
                }
            }
        }
        return esAdminDao;
    }
}
