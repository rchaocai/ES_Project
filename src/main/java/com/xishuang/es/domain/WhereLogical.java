package com.xishuang.es.domain;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import org.elasticsearch.index.query.QueryBuilder;

public class WhereLogical {
    SQLBinaryOperator operator;

    QueryBuilder left;

    QueryBuilder right;

    public SQLBinaryOperator getOperator() {
        return operator;
    }

    public void setOperator(SQLBinaryOperator operator) {
        this.operator = operator;
    }

    public QueryBuilder getLeft() {
        return left;
    }

    public void setLeft(QueryBuilder left) {
        this.left = left;
    }

    public QueryBuilder getRight() {
        return right;
    }

    public void setRight(QueryBuilder right) {
        this.right = right;
    }
}
