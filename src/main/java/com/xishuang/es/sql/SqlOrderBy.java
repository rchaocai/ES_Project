package com.xishuang.es.sql;

import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

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

    public boolean isOrderBy() {
        return sqlOrderBy != null;
    }

    public List<SQLSelectOrderByItem> getOrderList() {
        return sqlOrderBy.getItems();
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
