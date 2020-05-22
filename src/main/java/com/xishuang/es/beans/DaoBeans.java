package com.xishuang.es.beans;

import com.xishuang.es.admin.EsAdminDao;
import com.xishuang.es.dao.EsDao;
import com.xishuang.es.util.HttpUtil;
import com.xishuang.es.util.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DaoBeans {
    private static final Logger LOGGER = LogManager.getLogger(DaoBeans.class);

    /**
     * ES地址,ip:port
     */
    @Value("${elasticsearch.ip}")
    String ipPort;

    @Bean
    public EsDao esDao() {
        if (StringUtils.isBlank(ipPort)) {
            LOGGER.info("请检查es配置");
            return null;
        }
        return new EsDao(HttpUtil.getHttpHost(ipPort));
    }

    @Bean
    public EsAdminDao esAdminDao() {
        if (StringUtils.isBlank(ipPort)) {
            LOGGER.info("请检查es配置");
            return null;
        }
        return new EsAdminDao(HttpUtil.getHttpHost(ipPort));
    }

}
