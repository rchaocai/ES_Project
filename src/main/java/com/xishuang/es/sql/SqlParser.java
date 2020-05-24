package com.xishuang.es.sql;

import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;
import com.xishuang.es.dao.EsDao;
import com.xishuang.es.domain.EsCommonResult;
import com.xishuang.es.util.StringUtils;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * SQL解析器
 */
public class SqlParser {
    private final static String dbType = JdbcConstants.MYSQL;
    private final static Logger logger = LoggerFactory.getLogger(SqlParser.class);

    public static void parse(EsDao esDao, String sql) throws Exception {
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


        // 获取表名，只支持简单的单表查询
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);
        Map<TableStat.Name, TableStat> tablesMap = visitor.getTables();
        String tableName = null;
        for (Map.Entry<TableStat.Name, TableStat> entry : tablesMap.entrySet()) {
            tableName = entry.getKey().getName();
            if (tableName != null) {
                break;
            }
        }

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 针对select字段做处理
        List<SQLSelectItem> selectList = sqlSelectQueryBlock.getSelectList();
        setSelect(searchSourceBuilder, selectList);
        // 针对limit做处理
        SQLLimit sqlLimit = sqlSelectQueryBlock.getLimit();
        setLimit(searchSourceBuilder, sqlLimit);
        // 针对order做处理
        SQLOrderBy sqlOrderBy = sqlSelectQueryBlock.getOrderBy();
        setOrder(searchSourceBuilder, sqlOrderBy);
        // 针对where做处理
        SQLExpr whereExpr = sqlSelectQueryBlock.getWhere();
        QueryBuilder queryBuilder = setWhere(whereExpr);
        searchSourceBuilder.query(queryBuilder);
        // 具体查询请求
        EsCommonResult result = esDao.listToQueryResult(tableName, searchSourceBuilder);
        System.out.println("测试:" + result.getBeans().size());
    }


    /**
     * 针对select字段做处理
     */
    private static void setSelect(SearchSourceBuilder searchSourceBuilder, List<SQLSelectItem> selectList) {
        System.out.println(selectList);
        List<String> columnList = new ArrayList<>();
        for (SQLSelectItem selectItem : selectList) {
            if ("*".equals(selectItem.toString())) {
                break;
            } else {
                columnList.add(selectItem.toString());
            }
        }

        if (columnList.isEmpty()) return;
        searchSourceBuilder.fetchSource(columnList.toArray(new String[0]), null);
    }


    /**
     * 针对limit处理
     */
    private static void setLimit(SearchSourceBuilder searchSourceBuilder, SQLLimit sqlLimit) {
        if (sqlLimit == null) return;

        if (sqlLimit.getOffset() != null) {
            searchSourceBuilder.from(Integer.parseInt(sqlLimit.getOffset().toString()));
        }
        if (sqlLimit.getRowCount() != null) {
            searchSourceBuilder.size(Integer.parseInt(sqlLimit.getRowCount().toString()));
        }

        // 针对order进行处理
        System.out.println("测试" + sqlLimit.getRowCount());
        System.out.println("测试" + sqlLimit.getOffset());
    }

    /**
     * 针对order做处理
     */
    private static void setOrder(SearchSourceBuilder searchSourceBuilder, SQLOrderBy sqlOrderBy) {
        if (sqlOrderBy == null) return;

        List<SQLSelectOrderByItem> orderByList = sqlOrderBy.getItems();
        for (SQLSelectOrderByItem item : orderByList) {
            String orderBy = item.getExpr().toString();
            if (item.getType() == null) {
                searchSourceBuilder.sort(orderBy);
            } else {
                searchSourceBuilder.sort(orderBy,
                        item.getType().equals(SQLOrderingSpecification.ASC) ? SortOrder.ASC : SortOrder.DESC);
            }
        }
    }

    /**
     * 针对where做处理
     */
    private static QueryBuilder setWhere(SQLExpr whereExpr) throws Exception {
        if (whereExpr == null) return QueryBuilders.boolQuery();

        if (whereExpr instanceof SQLBinaryOpExpr) { // 二元运算
            SQLBinaryOperator operator = ((SQLBinaryOpExpr) whereExpr).getOperator(); // 获取运算符
            System.out.println(operator);
            if (operator.isLogical()) { // and,or,xor
                System.out.println(whereExpr);
                return handleLogicalExpr(whereExpr);
            } else if (operator.isRelational()) { // 具体的运算,位于叶子节点
                System.out.println(whereExpr);
                return handleRelationalExpr(whereExpr);
            }
        }

        return QueryBuilders.boolQuery();
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
            }
//            else if (operator.isRelational()) { // 具体的运算,位于叶子节点
//                return handleRelationalExpr(expr);
//            }
        }
//        else if (expr instanceof SQLBetweenExpr) { // between运算
//            SQLBetweenExpr between = ((SQLBetweenExpr) expr);
//            boolean isNotBetween = between.isNot(); // between or not between ?
//            String testExpr = between.testExpr.toString();
//            String fromStr = formatSQLValue(between.beginExpr.toString());
//            String toStr = formatSQLValue(between.endExpr.toString());
//            if (isNotBetween) {
//                bridge.must(QueryBuilders.rangeQuery(testExpr).lt(fromStr).gt(toStr));
//            } else {
//                bridge.must(QueryBuilders.rangeQuery(testExpr).gte(fromStr).lte(toStr));
//            }
//            return bridge;
//        }
//        else if (expr instanceof SQLInListExpr) { // SQL的 in语句，ES中对应的是terms
//            SQLInListExpr siExpr = (SQLInListExpr) expr;
//            boolean isNotIn = siExpr.isNot(); // in or not in?
//            String leftSide = siExpr.getExpr().toString();
//            List<SQLExpr> inSQLList = siExpr.getTargetList();
//            List<String> inList = new ArrayList<>();
//            for (SQLExpr in : inSQLList) {
//                String str = formatSQLValue(in.toString());
//                inList.add(str);
//            }
//            if (isNotIn) {
//                bridge.mustNot(QueryBuilders.termsQuery(leftSide, inList));
//            } else {
//                bridge.must(QueryBuilders.termsQuery(leftSide, inList));
//            }
//            return bridge;
//        }
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
        QueryBuilder leftBridge = setWhere(leftExpr);
        QueryBuilder rightBridge = setWhere(rightExpr);
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
