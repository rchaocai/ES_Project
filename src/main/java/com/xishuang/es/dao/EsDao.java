package com.xishuang.es.dao;

import com.alibaba.fastjson.JSONObject;
import com.xishuang.es.EsClientSingleton;
import com.xishuang.es.domain.EsCommonResult;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.LinkedList;
import java.util.List;

public class EsDao {
    private static final Logger LOGGER = LogManager.getLogger(EsDao.class);
    private RestHighLevelClient client;
    private long defaultSearchTimeoutMs = 30_000; // 小于等于0则代表不限制
    private Boolean defaultAllowPartialSearchResults = true; // null则代表不设置

    public EsDao(HttpHost... hosts) {
        LOGGER.info("init [{}]", this.getClass().getName());
        this.client = EsClientSingleton.getInstance(hosts);
    }

    /**
     * @param index               索引
     * @param searchSourceBuilder 查询语句
     */
    public EsCommonResult listToQueryResult(String index, SearchSourceBuilder searchSourceBuilder) {
        LOGGER.info("listToQueryResult => [{}]/[{}]", index, searchSourceBuilder.toString());
        StopWatch clock = new StopWatch();
        clock.start();
        SearchRequest searchRequest = setDefaultSearchParam(new SearchRequest(index).source(searchSourceBuilder));
        IndicesOptions fromOptions = IndicesOptions.fromOptions(true, true, true, false);
        fromOptions.ignoreUnavailable();
        searchRequest.indicesOptions(fromOptions);
        RequestOptions options = RequestOptions.DEFAULT;
        SearchResponse response;
        try {
            response = client.search(searchRequest, options);
            if (!checkResponse(response)) {
                LOGGER.error("search failed with response status[{}]", response.status().name());
                return null;
            }
            SearchHits hits = response.getHits();
            List<String> beans = new LinkedList<>();
            Object[] lastSortValues = null;
            for (SearchHit hit : hits) {
                String bean = hit.getSourceAsString();
                lastSortValues = hit.getSortValues();
                beans.add(bean);
            }

            EsCommonResult result = new EsCommonResult();
            result.setAmounts(hits.getTotalHits().value);
            result.setBeans(beans);
            result.setLastSortValues(lastSortValues);
            LOGGER.info("status is " + response.status().name() + ", took " + clock.totalTime() + "ms, timeout is " + response.isTimedOut() + ", hits.value is " + hits.getTotalHits().relation + " " + result.getAmounts() + ", bean amount is " + result.getBeans().size());
            return result;
        } catch (Exception e) {
            LOGGER.error("search es cause an exception", e);
            return null;
        }
    }

    private boolean checkResponse(SearchResponse response) {
        String statusName = response.status().name();
        return "CREATE".equals(statusName) || "OK".equals(statusName) || "CREATED".equals(statusName);
    }

    private SearchRequest setDefaultSearchParam(SearchRequest searchRequest) {
        SearchSourceBuilder searchSourceBuilder = searchRequest.source();
        if (searchSourceBuilder.timeout() == null && defaultSearchTimeoutMs > 0) {
            searchSourceBuilder.timeout(new TimeValue(defaultSearchTimeoutMs));
        }
        if (searchRequest.allowPartialSearchResults() == null && defaultAllowPartialSearchResults != null) {
            searchRequest.allowPartialSearchResults(defaultAllowPartialSearchResults);
        }
        return searchRequest;
    }
}
