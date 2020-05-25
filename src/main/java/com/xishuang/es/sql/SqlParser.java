package com.xishuang.es.sql;

import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;
import com.xishuang.es.dao.EsDao;
import com.xishuang.es.domain.EsCommonResult;
import com.xishuang.es.domain.WhereLogical;
import com.xishuang.es.util.StringUtils;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


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
        List<WhereLogical> whereLogicals = new ArrayList<>();
        QueryBuilder single = setWhere(whereLogicals, whereExpr);
        QueryBuilder boolQueryBuilder = setWhereConnect(whereLogicals);
        if (whereLogicals.isEmpty() && single != null) {
            searchSourceBuilder.query(single);
            boolQueryBuilder = single;
        } else if (!whereLogicals.isEmpty()) {
            searchSourceBuilder.query(boolQueryBuilder);
        }

        // 针对聚合
        SQLSelectGroupByClause groupByClause = sqlSelectQueryBlock.getGroupBy();
        if (groupByClause == null) { // 非分组聚合
            EsCommonResult result = esDao.listToQueryResult(tableName, searchSourceBuilder);
            System.out.println("测试:" + result.getBeans().size());
        } else { // 分组聚合
            List<SQLExpr> groupByList = groupByClause.getItems();
            if (groupByList.size() == 1) { // 单字段分组
                TermsAggregationBuilder termsAggregationBuilder = setGroupBySingleField(groupByClause);
                List<String> list = esDao.aggregateToQueryResult(tableName, boolQueryBuilder, termsAggregationBuilder);
                System.out.println("测试:" + list);
            } else { // 多字段分组
                List<CompositeValuesSourceBuilder<?>> sources = setGroupByMultiField(groupByList);
                List<String> list = esDao.groupByNoSort(tableName, boolQueryBuilder, sources);
                System.out.println("测试:" + list);
            }
        }

//        System.out.println(searchSourceBuilder.toString());
    }

    /**
     * 多字段聚合，group by语句进行处理
     */
    private static List<CompositeValuesSourceBuilder<?>> setGroupByMultiField(List<SQLExpr> groupByList) {
        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
        for (SQLExpr sqlExpr : groupByList) {
            String column = sqlExpr.toString();
            sources.add(new TermsValuesSourceBuilder(column).field(column).missingBucket(true));
        }

        return sources;
    }

    /**
     * 单字段聚合，group by语句进行处理
     */
    private static TermsAggregationBuilder setGroupBySingleField(SQLSelectGroupByClause groupByClause) {
        List<SQLExpr> groupByList = groupByClause.getItems();
        String column = groupByList.get(0).toString();
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms(column).field(column).size(1);

        // 简单对having做处理
        SQLExpr havingExpr = groupByClause.getHaving();
        if (havingExpr != null) {
            SQLExpr leftExpr = ((SQLBinaryOpExpr) havingExpr).getLeft();
            SQLExpr rightExpr = ((SQLBinaryOpExpr) havingExpr).getRight();
            String leftExprStr = leftExpr.toString();
            String rightExprStr = rightExpr.toString();

            termsAggregationBuilder.minDocCount(Long.parseLong(rightExprStr));
        }

        //
        termsAggregationBuilder.order(BucketOrder.count(true));

        return termsAggregationBuilder;
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
            } else if (!selectItem.toString().contains("(")) {
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
    private static QueryBuilder setWhere(List<WhereLogical> whereLogicals, SQLExpr whereExpr) throws Exception {
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
    private static BoolQueryBuilder setWhereConnect(List<WhereLogical> whereLogicals) {
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
    private static BoolQueryBuilder setWhereIn(SQLInListExpr siExpr) {
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
    private static QueryBuilder setWhereBetween(SQLBetweenExpr between) {
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
    private static void handleLogicalRecurse(List<WhereLogical> whereLogicals, SQLExpr expr) throws Exception {
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
    private static QueryBuilder setWhereRelational(SQLExpr expr) {
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

}
