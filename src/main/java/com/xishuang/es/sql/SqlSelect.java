package com.xishuang.es.sql;

import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

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
        List<String> columnList = getNormalColumnList();
        if (columnList.isEmpty()) return;
        searchSourceBuilder.fetchSource(columnList.toArray(new String[0]), null);
    }

    public void setSelect(InnerHitBuilder innerHitBuilder) {
        List<String> columnList = getNormalColumnList();
        if (columnList.isEmpty()) return;
        innerHitBuilder.setFetchSourceContext(new FetchSourceContext(true, columnList.toArray(new String[0]), null));
    }

    public void setSelect(TopHitsAggregationBuilder topHitsAggregateBuilder) {
        List<String> columnList = getNormalColumnList();
        if (columnList.isEmpty()) return;
        topHitsAggregateBuilder.fetchSource(columnList.toArray(new String[0]), null);
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

    /**
     * 获取正常聚合列表
     */
    public List<String> getNormalColumnList() {
        List<String> columnList = new ArrayList<>();
        for (SQLSelectItem selectItem : selectList) {
            if (isAllColumn(selectItem)) {
                break;
            } else if (isProperty(selectItem)) {
                columnList.add(selectItem.toString());
            }
        }

        return columnList;
    }

    /**
     * 获取正常聚合列表
     */
    public List<SQLSelectItem> getNoNormalColumnList() {
        List<SQLSelectItem> columnList = new ArrayList<>();
        for (SQLSelectItem selectItem : selectList) {
            if (isAllColumn(selectItem)) {
                break;
            } else if (!isProperty(selectItem)) {
                columnList.add(selectItem);
            }
        }

        return columnList;
    }

    /**
     * 获取方法函数列表
     */
    public List<SQLSelectItem> getMethodList() {
        List<SQLSelectItem> methodList = new ArrayList<>();
        for (SQLSelectItem item : selectList) {
            if (isMethodInvoke(item)) {
                methodList.add(item);
            }
        }
        return methodList;
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
     * 是否是方法调用字段
     */
    public boolean isMethodInvoke(SQLSelectItem selectItem) {
        return selectItem.getExpr() instanceof SQLMethodInvokeExpr;
    }

    /**
     * 是否是属性字段
     */
    public boolean isProperty(SQLSelectItem selectItem) {
        return selectItem.getExpr() instanceof SQLPropertyExpr || selectItem.getExpr() instanceof SQLIdentifierExpr;
    }

    /**
     * 获取字段名
     */
    public String getColumnName(SQLSelectItem selectItem) {
        return selectItem.getExpr().toString();
    }

}
