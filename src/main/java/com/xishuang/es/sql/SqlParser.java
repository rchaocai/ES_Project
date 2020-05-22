package com.xishuang.es.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.util.JdbcConstants;
import com.xishuang.es.util.StringUtils;
import org.elasticsearch.index.query.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


/**
 * SQL解析器
 */
public class SqlParser {
    private final static String dbType = JdbcConstants.MYSQL;
    private final static Logger logger = LoggerFactory.getLogger(SqlParser.class);

    public static void parse(String sql) throws Exception {
        if (StringUtils.isBlank(sql)) {
            throw new IllegalArgumentException("SQL不能为空");
        }
        sql = sql.trim().toLowerCase();

        // SQL语法解析
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatement();
        if (!(stmt instanceof SQLSelectStatement)) {
            throw new IllegalArgumentException("输入语句须为Select语句");
        }

        SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) stmt;
        SQLSelectQuery sqlSelectQuery = sqlSelectStatement.getSelect().getQuery();
        SQLSelectQueryBlock sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlSelectQuery;

        SQLExpr whereExpr = sqlSelectQueryBlock.getWhere();
        List<SQLSelectItem> selectList = sqlSelectQueryBlock.getSelectList();

        // 处理where
        BoolQueryBuilder queryBuilder = null;
        QueryBuilder whereBuilder = whereHelper(whereExpr);

        for (SQLSelectItem selectItem : selectList) {
            System.out.println(selectItem.getExpr());
        }
    }


    /**
     * 递归遍历“where”子树
     */
    private static QueryBuilder whereHelper(SQLExpr expr) throws Exception {
        if (Objects.isNull(expr)) {
            throw new NullPointerException("节点不能为空!");
        }
        BoolQueryBuilder bridge = QueryBuilders.boolQuery();
        if (expr instanceof SQLBinaryOpExpr) { // 二元运算
            SQLBinaryOperator operator = ((SQLBinaryOpExpr) expr).getOperator(); // 获取运算符
            if (operator.isLogical()) { // and,or,xor
                return handleLogicalExpr(expr);
            } else if (operator.isRelational()) { // 具体的运算,位于叶子节点
                return handleRelationalExpr(expr);
            }
        } else if (expr instanceof SQLBetweenExpr) { // between运算
            SQLBetweenExpr between = ((SQLBetweenExpr) expr);
            boolean isNotBetween = between.isNot(); // between or not between ?
            String testExpr = between.testExpr.toString();
            String fromStr = formatSQLValue(between.beginExpr.toString());
            String toStr = formatSQLValue(between.endExpr.toString());
            if (isNotBetween) {
                bridge.must(QueryBuilders.rangeQuery(testExpr).lt(fromStr).gt(toStr));
            } else {
                bridge.must(QueryBuilders.rangeQuery(testExpr).gte(fromStr).lte(toStr));
            }
            return bridge;
        } else if (expr instanceof SQLInListExpr) { // SQL的 in语句，ES中对应的是terms
            SQLInListExpr siExpr = (SQLInListExpr) expr;
            boolean isNotIn = siExpr.isNot(); // in or not in?
            String leftSide = siExpr.getExpr().toString();
            List<SQLExpr> inSQLList = siExpr.getTargetList();
            List<String> inList = new ArrayList<>();
            for (SQLExpr in : inSQLList) {
                String str = formatSQLValue(in.toString());
                inList.add(str);
            }
            if (isNotIn) {
                bridge.mustNot(QueryBuilders.termsQuery(leftSide, inList));
            } else {
                bridge.must(QueryBuilders.termsQuery(leftSide, inList));
            }
            return bridge;
        }
        return bridge;
    }


    /**
     * 逻辑运算符，目前支持and,or
     */
    private static QueryBuilder handleLogicalExpr(SQLExpr expr) throws Exception {
        BoolQueryBuilder bridge = QueryBuilders.boolQuery();
        SQLBinaryOperator operator = ((SQLBinaryOpExpr) expr).getOperator(); // 获取运算符
        SQLExpr leftExpr = ((SQLBinaryOpExpr) expr).getLeft();
        SQLExpr rightExpr = ((SQLBinaryOpExpr) expr).getRight();

        // 分别递归左右子树，再根据逻辑运算符将结果归并
        QueryBuilder leftBridge = whereHelper(leftExpr);
        QueryBuilder rightBridge = whereHelper(rightExpr);
        if (operator.equals(SQLBinaryOperator.BooleanAnd)) {
            bridge.must(leftBridge).must(rightBridge);
        } else if (operator.equals(SQLBinaryOperator.BooleanOr)) {
            bridge.should(leftBridge).should(rightBridge);
        }
        return bridge;
    }


    /**
     * 大于小于等于正则
     */
    private static QueryBuilder handleRelationalExpr(SQLExpr expr) {
        SQLExpr leftExpr = ((SQLBinaryOpExpr) expr).getLeft();
        if (Objects.isNull(leftExpr)) {
            throw new NullPointerException("表达式左侧不得为空");
        }
        String leftExprStr = leftExpr.toString();
        String rightExprStr = formatSQLValue(((SQLBinaryOpExpr) expr).getRight().toString()); // TODO:表达式右侧可以后续支持方法调用
        SQLBinaryOperator operator = ((SQLBinaryOpExpr) expr).getOperator(); // 获取运算符
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
                queryBuilder = QueryBuilders.boolQuery();
                RegexpQueryBuilder regCond = QueryBuilders.regexpQuery(leftExprStr, rightExprStr);
                ((BoolQueryBuilder) queryBuilder).mustNot(regCond);
                break;
            case NotRegExp:
                queryBuilder = QueryBuilders.boolQuery();
                RegexpQueryBuilder notRegCond = QueryBuilders.regexpQuery(leftExprStr, rightExprStr);
                ((BoolQueryBuilder) queryBuilder).mustNot(notRegCond);
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

}
