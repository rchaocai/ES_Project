package com.xishuang.es.sql;

import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.xishuang.es.util.SqlUtil;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.List;

public class SqlOrderBy {

    SQLOrderBy sqlOrderBy;

    public SqlOrderBy(SQLSelectQueryBlock sqlSelectQueryBlock) {
        this.sqlOrderBy = sqlSelectQueryBlock.getOrderBy();
    }

    public SqlOrderBy(SQLOrderBy sqlOrderBy) {
        this.sqlOrderBy = sqlOrderBy;
    }

    /**
     * 针对order做处理
     */
    public void setOrder(SearchSourceBuilder searchSourceBuilder) {
        if (sqlOrderBy == null) return;

        List<SQLSelectOrderByItem> orderByList = sqlOrderBy.getItems();
        for (SQLSelectOrderByItem item : orderByList) {
            String orderBy = item.getExpr().toString();
            searchSourceBuilder.sort(orderBy, isASC(item) ? SortOrder.ASC : SortOrder.DESC);
        }
    }

    /**
     * 针对order做处理
     */
    public void setOrder(TopHitsAggregationBuilder topHitsAggregationBuilder) {
        if (sqlOrderBy == null) return;

        List<SQLSelectOrderByItem> orderByList = sqlOrderBy.getItems();
        for (SQLSelectOrderByItem item : orderByList) {
            String orderBy = item.getExpr().toString();
            topHitsAggregationBuilder.sort(orderBy, isASC(item) ? SortOrder.ASC : SortOrder.DESC);
        }
    }

    public void setOrder(InnerHitBuilder innerHitBuilder) {
        if (sqlOrderBy == null) return;

        List<SQLSelectOrderByItem> orderByList = sqlOrderBy.getItems();
        for (SQLSelectOrderByItem item : orderByList) {
            String orderBy = item.getExpr().toString();
            innerHitBuilder.addSort(SortBuilders.fieldSort(orderBy).order(isASC(item) ? SortOrder.ASC : SortOrder.DESC));
        }
    }

    /**
     * @param aggSelectList select的聚合函数参数列表
     * @param groupByColumn 分组字段
     */
    public void setOrder(TermsAggregationBuilder termsAggregationBuilder, List<SQLSelectItem> aggSelectList, String groupByColumn) {
        if (sqlOrderBy == null) return;

        List<SQLSelectOrderByItem> orderByList = sqlOrderBy.getItems();
        List<BucketOrder> bucketOrderList = new ArrayList<>();
        for (SQLSelectOrderByItem item : orderByList) {
            String orderBy = item.getExpr().toString();

            if (SqlUtil.isColumnMatch(aggSelectList, orderBy)) { // 排序字段是否是聚合后的字段
                bucketOrderList.add(BucketOrder.count(isASC(item)));
            } else if (groupByColumn.equals(orderBy)) { // 排序字段是否是分组字段
                bucketOrderList.add(BucketOrder.key(isASC(item)));
            }
        }
        if (!bucketOrderList.isEmpty()) {
            termsAggregationBuilder.order(BucketOrder.compound(bucketOrderList));
        }
    }

    /**
     * @param aggSelectList select的聚合函数参数列表
     * @param groupByColumn 分组字段
     */
    public void setOrder(DateHistogramAggregationBuilder dateHistogramAggregationBuilder, List<SQLSelectItem> aggSelectList, String groupByColumn) {
        if (sqlOrderBy == null) return;

        List<SQLSelectOrderByItem> orderByList = sqlOrderBy.getItems();
        List<BucketOrder> bucketOrderList = new ArrayList<>();
        for (SQLSelectOrderByItem item : orderByList) {
            String orderBy = item.getExpr().toString();

            if (SqlUtil.isColumnMatch(aggSelectList, orderBy)) { // 排序字段是否是聚合后的字段
                bucketOrderList.add(BucketOrder.count(isASC(item)));
            } else if (groupByColumn.equals(orderBy)) { // 排序字段是否是分组字段
                bucketOrderList.add(BucketOrder.key(isASC(item)));
            }
        }
        if (!bucketOrderList.isEmpty()) {
            dateHistogramAggregationBuilder.order(BucketOrder.compound(bucketOrderList));
        }
    }

    public boolean isOrderBy() {
        return sqlOrderBy != null;
    }

    /**
     * mysql不显示指定的话默认升序
     */
    public boolean isASC(SQLSelectOrderByItem item) {
        if (item.getType() == null) {
            return true;
        } else {
            return item.getType().equals(SQLOrderingSpecification.ASC);
        }
    }

}
