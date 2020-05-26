package com.xishuang.es.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.xishuang.es.domain.WhereLogical;
import org.elasticsearch.index.query.*;

import java.util.ArrayList;
import java.util.List;

public class SqlWhere {

    SQLExpr whereExpr;

    public SqlWhere(SQLSelectQueryBlock sqlSelectQueryBlock) {
        this.whereExpr = sqlSelectQueryBlock.getWhere();
    }

    public SQLExpr getWhereExpr() {
        return whereExpr;
    }

    /**
     * 针对where做处理
     */
    public QueryBuilder setWhere(List<WhereLogical> whereLogicals, SQLExpr whereExpr) throws Exception {
        if (whereExpr == null) return null;
        if (whereExpr instanceof SQLBinaryOpExpr) { // 二元运算
            SQLBinaryOperator operator = ((SQLBinaryOpExpr) whereExpr).getOperator(); // 获取运算符
            System.out.println(operator);
            if (operator.isLogical()) {
                // 需要递归遍历等式两端，and,or,xor
                handleLogicalRecurse(whereLogicals, whereExpr);
            } else if (operator.isRelational()) {
                // 具体的运算,位于叶子节点
                return setWhereRelational(whereExpr);
            }
        } else if (whereExpr instanceof SQLInListExpr) {
            // SQL的 in语句
            return setWhereIn((SQLInListExpr) whereExpr);
        } else if (whereExpr instanceof SQLBetweenExpr) {
            // between运算
            return setWhereBetween(((SQLBetweenExpr) whereExpr));
        }

        return null;
    }

    /**
     * 利用whereLogicals进行查询条件拼接，减少嵌套
     *
     * @param whereLogicals 查询条件列表
     */
    public BoolQueryBuilder setWhereConnect(List<WhereLogical> whereLogicals) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        List<QueryBuilder> filters = boolQueryBuilder.filter();
        System.out.println(whereLogicals.size());
        for (WhereLogical whereLogical : whereLogicals) {
            QueryBuilder left = whereLogical.getLeft();
            QueryBuilder right = whereLogical.getRight();
            if (whereLogical.getOperator().equals(SQLBinaryOperator.BooleanAnd)) {
//                if (left != null) boolQueryBuilder.must(left);
//                if (right != null) boolQueryBuilder.must(right);
                if (left != null) filters.add(left);
                if (right != null) filters.add(right);
            } else if (whereLogical.getOperator().equals(SQLBinaryOperator.BooleanOr)) {
                if (left != null) boolQueryBuilder.should(left);
                if (right != null) boolQueryBuilder.should(right);
            }
        }

        return boolQueryBuilder;
    }

    /**
     * 针对where中的in语句，ES中对应的是terms
     */
    private BoolQueryBuilder setWhereIn(SQLInListExpr siExpr) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        boolean isNotIn = siExpr.isNot(); // in or not in?
        String leftSide = siExpr.getExpr().toString();
        List<SQLExpr> inSQLList = siExpr.getTargetList();
        List<String> inList = new ArrayList<>();
        for (SQLExpr in : inSQLList) {
            String str = in.toString().replace("'", "");
            inList.add(str);
        }
        if (isNotIn) {
            boolQuery.mustNot(QueryBuilders.termsQuery(leftSide, inList));
        } else {
            boolQuery.filter(QueryBuilders.termsQuery(leftSide, inList));
        }

        return boolQuery;
    }

    /**
     * 针对where中的between语句
     */
    private QueryBuilder setWhereBetween(SQLBetweenExpr between) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        // between or not between
        boolean isNotBetween = between.isNot();
        String testExpr = between.testExpr.toString().replace("'", "");
        String fromStr = between.beginExpr.toString().replace("'", "");
        String toStr = between.endExpr.toString();
        if (isNotBetween) {
            boolQuery.should(QueryBuilders.rangeQuery(testExpr).lt(fromStr));
            boolQuery.should(QueryBuilders.rangeQuery(testExpr).gt(toStr));
        } else {
            boolQuery.filter(QueryBuilders.rangeQuery(testExpr).gte(fromStr).lte(toStr));
        }

        return boolQuery;
    }


    /**
     * 逻辑运算符，目前支持and,or
     */
    private void handleLogicalRecurse(List<WhereLogical> whereLogicals, SQLExpr expr) throws Exception {
        SQLBinaryOperator operator = ((SQLBinaryOpExpr) expr).getOperator(); // 获取运算符
        SQLExpr leftExpr = ((SQLBinaryOpExpr) expr).getLeft();
        SQLExpr rightExpr = ((SQLBinaryOpExpr) expr).getRight();

        // 分别递归左右子树，再根据逻辑运算符将结果归并
        QueryBuilder left = setWhere(whereLogicals, leftExpr);
        QueryBuilder right = setWhere(whereLogicals, rightExpr);

        WhereLogical whereLogical = new WhereLogical();
        whereLogical.setOperator(operator);
        whereLogical.setLeft(left);
        whereLogical.setRight(right);
        whereLogicals.add(whereLogical);
    }


    /**
     * 大于小于等于正则
     */
    private QueryBuilder setWhereRelational(SQLExpr expr) {
        SQLExpr leftExpr = ((SQLBinaryOpExpr) expr).getLeft();
        SQLExpr rightExpr = ((SQLBinaryOpExpr) expr).getRight();
        String leftExprStr = leftExpr.toString();
        String rightExprStr = rightExpr.toString();
        rightExprStr = rightExprStr.replace("'", "");
        System.out.println(leftExprStr + ":" + rightExprStr);
        // 获取运算符
        SQLBinaryOperator operator = ((SQLBinaryOpExpr) expr).getOperator();
        QueryBuilder queryBuilder;
        switch (operator) {
            case GreaterThanOrEqual:
                queryBuilder = QueryBuilders.rangeQuery(leftExprStr).gte(rightExprStr);
                break;
            case LessThanOrEqual:
                queryBuilder = QueryBuilders.rangeQuery(leftExprStr).lte(rightExprStr);
                break;
            case Equality:
                queryBuilder = QueryBuilders.boolQuery();
                TermQueryBuilder eqCond = QueryBuilders.termQuery(leftExprStr, rightExprStr);
                ((BoolQueryBuilder) queryBuilder).must(eqCond);
                break;
            case GreaterThan:
                queryBuilder = QueryBuilders.rangeQuery(leftExprStr).gt(rightExprStr);
                break;
            case LessThan:
                queryBuilder = QueryBuilders.rangeQuery(leftExprStr).lt(rightExprStr);
                break;
            case NotEqual:
                queryBuilder = QueryBuilders.boolQuery();
                TermQueryBuilder notEqCond = QueryBuilders.termQuery(leftExprStr, rightExprStr);
                ((BoolQueryBuilder) queryBuilder).mustNot(notEqCond);
                break;
            case RegExp: // 对应到ES中的正则查询
            case NotRegExp:
                queryBuilder = QueryBuilders.boolQuery();
                RegexpQueryBuilder regCond = QueryBuilders.regexpQuery(leftExprStr, rightExprStr);
                ((BoolQueryBuilder) queryBuilder).mustNot(regCond);
                break;
            case Like:
                queryBuilder = QueryBuilders.boolQuery();
                MatchPhraseQueryBuilder likeCond = QueryBuilders.matchPhraseQuery(leftExprStr,
                        rightExprStr.replace("%", ""));
                ((BoolQueryBuilder) queryBuilder).must(likeCond);
                break;
            case NotLike:
                queryBuilder = QueryBuilders.boolQuery();
                MatchPhraseQueryBuilder notLikeCond = QueryBuilders.matchPhraseQuery(leftExprStr,
                        rightExprStr.replace("%", ""));
                ((BoolQueryBuilder) queryBuilder).mustNot(notLikeCond);
                break;
            default:
                throw new IllegalArgumentException("暂不支持该运算符!" + operator.toString());
        }
        return queryBuilder;
    }

    public int getRankNum(String row_number_alias) {
        SQLExpr leftExpr = ((SQLBinaryOpExpr) whereExpr).getLeft();
        SQLExpr rightExpr = ((SQLBinaryOpExpr) whereExpr).getRight();
        String leftExprStr = leftExpr.toString();
        String rightExprStr = rightExpr.toString();

        if (row_number_alias.equals(leftExprStr)) {
            return Integer.parseInt(rightExprStr);
        }

        return 1;
    }
}
