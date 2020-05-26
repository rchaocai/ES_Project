package com.xishuang.es.sql;

import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.List;

public class SqlSelect {
    List<SQLSelectItem> selectList;

    public SqlSelect(SQLSelectQueryBlock sqlSelectQueryBlock) {
        this.selectList = sqlSelectQueryBlock.getSelectList();
    }

    /**
     * 针对select字段做处理
     */
    public void setSelect(SearchSourceBuilder searchSourceBuilder) {
        System.out.println(selectList);
        List<String> columnList = new ArrayList<>();
        for (SQLSelectItem selectItem : selectList) {
            if (isAllColumn(selectItem)) {
                break;
            } else if (!isAggregate(selectItem)) {
                columnList.add(selectItem.toString());
            }
        }
        if (columnList.isEmpty()) return;
        searchSourceBuilder.fetchSource(columnList.toArray(new String[0]), null);
    }

    public List<SQLSelectItem> getSelectList() {
        return selectList;
    }

    /**
     * 是否选择全部字段
     */
    private boolean isAllColumn(SQLSelectItem selectItem) {
        return selectItem.getExpr() instanceof SQLAllColumnExpr;
    }

    /**
     * 是否是聚合函数字段
     */
    public boolean isAggregate(SQLSelectItem selectItem) {
        return selectItem.getExpr() instanceof SQLAggregateExpr;
    }

    /**
     * 获取字段名
     */
    public String getColumnName(SQLSelectItem selectItem) {
        return selectItem.getExpr().toString();
    }

    /**
     * 获取聚合函数列表
     */
    public List<SQLSelectItem> getAggList() {
        List<SQLSelectItem> aggList = new ArrayList<>();
        for (SQLSelectItem item : selectList) {
            if (isAggregate(item)) {
                aggList.add(item);
            }
        }

        return aggList;
    }

}
