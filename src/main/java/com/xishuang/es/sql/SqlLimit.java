package com.xishuang.es.sql;

import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class SqlLimit {
    SQLLimit sqlLimit;

    public SqlLimit(SQLSelectQueryBlock sqlSelectQueryBlock) {
        this.sqlLimit = sqlSelectQueryBlock.getLimit();
    }

    public boolean isLimit() {
        return sqlLimit != null;
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


    public int getRowCount() {
        return Integer.parseInt(sqlLimit.getRowCount().toString());
    }
}
