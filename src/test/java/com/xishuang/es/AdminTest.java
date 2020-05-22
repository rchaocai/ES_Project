package com.xishuang.es;

import com.xishuang.es.admin.EsAdminDao;
import com.xishuang.es.util.HttpUtil;

import java.io.IOException;

public class AdminTest {

    public static void main(String[] args) throws IOException {
        EsAdminDao esAdminDao = new EsAdminDao(HttpUtil.getHttpHost("47.107.187.164:9200"));
        esAdminDao.getIndexConfig("metrics_index");
    }
}
