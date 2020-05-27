package com.xishuang.es.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLOver;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;

import java.util.ArrayList;
import java.util.List;

/**
 * 针对row_number函数进行处理，主要用于取top n
 */
public class SqlFuncRowNumber {
    private final String alias;
    private final SQLOver sqlOver;

    public SqlFuncRowNumber(SQLSelectItem item) {
        this.alias = item.getAlias();
        SQLAggregateExpr aggregateExpr = (SQLAggregateExpr) item.getExpr();
        this.sqlOver = aggregateExpr.getOver();
    }

    public String getFirstPartitionBy() {
        return sqlOver.getPartitionBy().get(0).toString();
    }

    public List<String> getPartitionByList() {
        List<String> list = new ArrayList<>();
        for (SQLExpr expr: sqlOver.getPartitionBy()) {
            list.add(expr.toString());
        }
        return list;
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
