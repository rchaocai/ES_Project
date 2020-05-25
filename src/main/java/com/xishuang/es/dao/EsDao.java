package com.xishuang.es.dao;

import com.alibaba.fastjson.JSONObject;
import com.xishuang.es.EsClientSingleton;
import com.xishuang.es.domain.EsCommonResult;
import com.xishuang.es.util.StringUtils;
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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.lang.Nullable;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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

    /**
     * count() group by语句进行处理，不排序
     */
    public List<String> groupByNoSort(String index, QueryBuilder queryBuilder, List<CompositeValuesSourceBuilder<?>> sources) {
        LOGGER.info("aggregate term all => [{}]/[{}][{}]", index, queryBuilder.toString(), sources);
        List<String> aggregationResults = new ArrayList<>();
        final int batchSize = 10000;
        Map<String, Object> afterKey = null;
        while (true) {
            CompositeAggregationBuilder composite = AggregationBuilders.composite("my_buckets", sources);
            composite.size(batchSize).aggregateAfter(afterKey);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                    .query(queryBuilder)
                    .aggregation(composite);
            Aggregation aggregate = aggregate(index, searchSourceBuilder);
            if (aggregate == null) {
                return new ArrayList<>();
            }
            ParsedComposite parsedComposite = (ParsedComposite) aggregate;
            afterKey = parsedComposite.afterKey();
            List<ParsedComposite.ParsedBucket> buckets = parsedComposite.getBuckets();
            for (ParsedComposite.ParsedBucket bucket : buckets) {
                Map<String, Object> map = bucket.getKey();
                map.put("doc_count", bucket.getDocCount());
                aggregationResults.add(map.toString());
            }
            if (buckets.size() < batchSize) {
                break;
            }
        }
        return aggregationResults;
    }

    public List<String> aggregateToQueryResult(@Nullable String index, QueryBuilder queryBuilder, AggregationBuilder aggregationBuilder) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder)
                .trackTotalHits(false)
                .size(0)
                .aggregation(aggregationBuilder);
        Aggregation aggregate = aggregate(index, searchSourceBuilder);
        if (aggregate == null) {
            return new ArrayList<>();
        }
        MultiBucketsAggregation multiBucketsAggregation = (MultiBucketsAggregation) aggregate;
        return bucketsToAggregationResults(multiBucketsAggregation.getBuckets());
    }

    protected Aggregation aggregate(String index, SearchSourceBuilder searchSourceBuilder) {
        LOGGER.info("aggregate => [{}]/[{}]", index, searchSourceBuilder);
        if (searchSourceBuilder.aggregations().count() == 0) {
            LOGGER.error("searchSourceBuilder.aggregations() can't empty");
            return null;
        }
        StopWatch clock = new StopWatch();
        clock.start();
        searchSourceBuilder.trackTotalHits(false).size(0).trackTotalHitsUpTo(10);
        SearchRequest searchRequest = setDefaultSearchParam(new SearchRequest(index).source(searchSourceBuilder));
        IndicesOptions fromOptions = IndicesOptions.fromOptions(true, true, true, false);
        fromOptions.ignoreUnavailable();
        searchRequest.indicesOptions(fromOptions);
        RequestOptions options = RequestOptions.DEFAULT;
        SearchResponse response;
        try {
            response = client.search(searchRequest, options);
            if (!"OK".equals(response.status().name())) {
                LOGGER.error("search failed with response status[{}]", response.status().name());
                return null;
            }
            Aggregations agg = response.getAggregations();
            if (agg == null) {
                return null;
            }
            List<Aggregation> aggregations = agg.asList();
            if (aggregations.isEmpty()) {
                return null;
            }
            Aggregation aggregation = aggregations.get(0);
            checkResponse(response);
            LOGGER.info("status is " + response.status().name() + ", took " + clock.totalTime() + "ms, timeout is " + response.isTimedOut() + ", bucket amount is " + ((MultiBucketsAggregation) aggregation).getBuckets().size());
            return aggregation;
        } catch (Exception e) {
            LOGGER.error("search es cause an exception", e);
            return null;
        }
    }

    private List<String> bucketsToAggregationResults(List<? extends MultiBucketsAggregation.Bucket> list) {
        List<String> aggregationResults = new ArrayList<>();
        for (MultiBucketsAggregation.Bucket b : list) {
            aggregationResults.add(b.getKeyAsString() + ":" + b.getDocCount());
        }
        return aggregationResults;
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
