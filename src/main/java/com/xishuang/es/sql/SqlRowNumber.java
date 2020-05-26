package com.xishuang.es.sql;

import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLOver;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;

public class SqlRowNumber {
    private String alias;
    private SQLOver sqlOver;

    public SqlRowNumber(SQLSelectItem item) {
        this.alias = item.getAlias();
        SQLAggregateExpr aggregateExpr = (SQLAggregateExpr) item.getExpr();
        this.sqlOver = aggregateExpr.getOver();
    }

    public String getPartitionBy() {
        return sqlOver.getPartitionBy().get(0).toString();
    }

    public String getAlias() {
        return alias;
    }

    public SQLOver getSqlOver() {
        return sqlOver;
    }

    public SQLOrderBy getOrderBy() {
        return sqlOver.getOrderBy();
    }
}
