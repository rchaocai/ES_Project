package com.xishuang.es.sql;

import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class SqlLimit {
    SQLLimit sqlLimit;

    public SqlLimit(SQLSelectQueryBlock sqlSelectQueryBlock) {
        this.sqlLimit = sqlSelectQueryBlock.getLimit();
    }

    /**
     * 针对limit处理
     */
    public void setLimit(SearchSourceBuilder searchSourceBuilder) {
        if (sqlLimit == null) return;

        if (sqlLimit.getOffset() != null) {
            searchSourceBuilder.from(Integer.parseInt(sqlLimit.getOffset().toString()));
        }
        if (sqlLimit.getRowCount() != null) {
            searchSourceBuilder.size(Integer.parseInt(sqlLimit.getRowCount().toString()));
        }
    }

    /**
     * 针对limit处理
     */
    public void setLimit(TermsAggregationBuilder termsAggregationBuilder) {
        if (sqlLimit == null) return;

        if (sqlLimit.getRowCount() != null) {
            termsAggregationBuilder.size(Integer.parseInt(sqlLimit.getRowCount().toString()));
        }
    }
}
