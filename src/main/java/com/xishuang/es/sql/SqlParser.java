package com.xishuang.es.sql;

import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.xishuang.es.dao.EsDao;
import com.xishuang.es.domain.*;
import com.xishuang.es.util.SqlUtil;
import com.xishuang.es.util.StringUtils;
import org.elasticsearch.index.query.*;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * SQL解析器
 */
public class SqlParser {
    private final static Logger logger = LoggerFactory.getLogger(SqlParser.class);

    public static List<? extends Object> parse(EsDao esDao, String sql, Result<List<?>> result) throws Exception {
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


        SQLTableSource sqlTableSource = sqlSelectQueryBlock.getFrom();
        if (sqlTableSource instanceof SQLSubqueryTableSource) { // 是否存在子查询，主要针对row_number函数进行处理
            SQLSubqueryTableSource sqlSubqueryTableSource = (SQLSubqueryTableSource) sqlTableSource;
            SQLSelectQuery subSqlSelectQuery = sqlSubqueryTableSource.getSelect().getQuery();
            SQLSelectQueryBlock subSqlSelectQueryBlock = (SQLSelectQueryBlock) subSqlSelectQuery;
            return parseSubQuery(esDao, subSqlSelectQueryBlock, sqlSelectQueryBlock, result);
//            return parseSubQueryForCollapse(esDao, subSqlSelectQueryBlock, sqlSelectQueryBlock, result);
        } else { // 正常的查询
            return parse(esDao, sqlSelectQueryBlock, result);
        }
    }

    private static List<? extends Object> parse(EsDao esDao, SQLSelectQueryBlock sqlSelectQueryBlock, Result<List<?>> result) throws Exception {
        String tableName = sqlSelectQueryBlock.getFrom().toString();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 针对select字段做处理
        SqlSelect sqlSelect = new SqlSelect(sqlSelectQueryBlock);
        sqlSelect.setSelect(searchSourceBuilder);
        // 针对limit做处理
        SqlLimit sqlLimit = new SqlLimit(sqlSelectQueryBlock);
        sqlLimit.setLimit(searchSourceBuilder);
        // 针对order做处理
        SqlOrderBy sqlOrderBy = new SqlOrderBy(sqlSelectQueryBlock);
        sqlOrderBy.setOrder(searchSourceBuilder);
        // 针对where做处理
        SqlWhere sqlWhere = new SqlWhere(sqlSelectQueryBlock);
        List<WhereLogical> whereLogicals = new ArrayList<>();
        QueryBuilder single = sqlWhere.setWhere(whereLogicals, sqlWhere.getWhereExpr());
        QueryBuilder boolQueryBuilder = sqlWhere.setWhereConnect(whereLogicals);
        if (whereLogicals.isEmpty() && single != null) {
            searchSourceBuilder.query(single);
            boolQueryBuilder = single;
        } else if (!whereLogicals.isEmpty()) {
            searchSourceBuilder.query(boolQueryBuilder);
        }

        // 针对聚合
        SqlGroupBy sqlGroupBy = new SqlGroupBy(sqlSelectQueryBlock);
        if (!sqlGroupBy.isGroupBy()) { // 非分组聚合
            List<EsCommonResult> list = new ArrayList<>();
            EsCommonResult esCommonResult = esDao.queryList(tableName, searchSourceBuilder, result);
            logger.debug("测试:" + esCommonResult.getBeans());
            list.add(esCommonResult);
            return Collections.singletonList(list);
        } else { // 分组聚合
            List<EsGroupByResult> list;
            if (sqlGroupBy.isMultiGroup() && !sqlOrderBy.isOrderBy()) { // 多字段分组且不需排序
                List<CompositeValuesSourceBuilder<?>> sources = sqlGroupBy.setGroupByMultiField();
                list = esDao.groupByNoSort(tableName, boolQueryBuilder, sources, result);
            } else { // 单字段分组
                AggregationBuilder aggregationBuilder = sqlGroupBy.setGroupBySingleField(sqlSelect, sqlOrderBy, sqlLimit);
                list = esDao.groupBy(tableName, boolQueryBuilder, aggregationBuilder, result);
            }
            logger.debug("测试:" + list);

            return list;
        }
    }

    /**
     * 分组top n计算:aggregate+topHit方式，通过script可以实现多字段分组
     */
    private static List<? extends Object> parseSubQuery(EsDao esDao, SQLSelectQueryBlock subQueryBlock, SQLSelectQueryBlock parentQueryBlock, Result<List<?>> result) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 获取表名
        String tableName = subQueryBlock.getFrom().toString();
        logger.debug("表名:" + tableName);
        // 针对select字段做处理
        SqlSelect sqlSelect = new SqlSelect(subQueryBlock);
        sqlSelect.setSelect(searchSourceBuilder);
        SQLSelectItem selectItem = sqlSelect.getAggList().get(0);
        SQLAggregateExpr aggregateExpr = (SQLAggregateExpr) selectItem.getExpr();
        // 目前只支持对row_number函数做处理
        if (!"row_number".equals(aggregateExpr.getMethodName())) {
            throw new IllegalArgumentException("只支持对row_number函数做处理");
        }
        SqlFuncRowNumber sqlRowNumber = new SqlFuncRowNumber(selectItem);
        String row_number_alias = sqlRowNumber.getAlias();
        List<String> partitionByList = sqlRowNumber.getPartitionByList();
        if (partitionByList.isEmpty()) {
            throw new IllegalArgumentException("row_number函数的partitionBy不能为空");
        }
        SqlOrderBy sqlOrderBy = new SqlOrderBy(sqlRowNumber.getOrderBy());
        // 针对where做处理
        SqlWhere sqlWhere = new SqlWhere(subQueryBlock);
        List<WhereLogical> whereLogicals = new ArrayList<>();
        QueryBuilder single = sqlWhere.setWhere(whereLogicals, sqlWhere.getWhereExpr());
        QueryBuilder boolQueryBuilder = sqlWhere.setWhereConnect(whereLogicals);
        if (whereLogicals.isEmpty() && single != null) {
            searchSourceBuilder.query(single);
            boolQueryBuilder = single;
        } else if (!whereLogicals.isEmpty()) {
            searchSourceBuilder.query(boolQueryBuilder);
        }

        // 取外部rank的限制数量
        SqlWhere parentSqlWhere = new SqlWhere(parentQueryBlock);
        int row_number = parentSqlWhere.getRankNum(row_number_alias);

        TermsAggregationBuilder termsAggregate;
        if (partitionByList.size() > 1) {
            Script script = new Script(SqlUtil.combineGroupBy2(partitionByList, "#"));
            termsAggregate = AggregationBuilders.terms("multiPartition").script(script);
        } else {
            termsAggregate = AggregationBuilders.terms(partitionByList.get(0)).field(partitionByList.get(0));
        }
        // 限制分组的数量
        SqlLimit sqlLimit = new SqlLimit(subQueryBlock);
        sqlLimit.setLimit(termsAggregate);

        TopHitsAggregationBuilder topHitsAggregateBuilder = AggregationBuilders.topHits("top")
                .size(row_number); // 分组内排序后取得数量
        sqlOrderBy.setOrder(topHitsAggregateBuilder);
        sqlSelect.setSelect(topHitsAggregateBuilder);

        List<EsGroupByTopNResult> esGroupResults = esDao.topNGroupByAgg(tableName, boolQueryBuilder, termsAggregate, topHitsAggregateBuilder, result);
        logger.debug("测试:" + esGroupResults);
        return Collections.singletonList(esGroupResults);
    }

    /**
     * 分组top n计算:collapse方式，只能单字段分组
     */
    private static List<Object> parseSubQueryForCollapse(EsDao esDao, SQLSelectQueryBlock subQueryBlock, SQLSelectQueryBlock parentQueryBlock, Result<List<?>> result) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 获取表名
        String tableName = subQueryBlock.getFrom().toString();
        System.out.println(tableName);
        // 针对select字段做处理
        SqlSelect parentSqlSelect = new SqlSelect(parentQueryBlock);
        parentSqlSelect.setSelect(searchSourceBuilder);

        SqlSelect subSqlSelect = new SqlSelect(subQueryBlock);
        SQLSelectItem selectItem = subSqlSelect.getAggList().get(0);
        SQLAggregateExpr aggregateExpr = (SQLAggregateExpr) selectItem.getExpr();
        // 目前只支持对row_number函数做处理
        if (!"row_number".equals(aggregateExpr.getMethodName())) {
            throw new IllegalArgumentException("只支持对row_number函数做处理");
        }
        SqlFuncRowNumber sqlRowNumber = new SqlFuncRowNumber(selectItem);
        String row_number_alias = sqlRowNumber.getAlias();
        String partitionBy = sqlRowNumber.getFirstPartitionBy();
        SqlOrderBy sqlOrderBy = new SqlOrderBy(sqlRowNumber.getOrderBy());
        // 针对limit做处理
        SqlLimit sqlLimit = new SqlLimit(subQueryBlock);
        sqlLimit.setLimit(searchSourceBuilder);
        // 针对where做处理
        SqlWhere sqlWhere = new SqlWhere(subQueryBlock);
        List<WhereLogical> whereLogicals = new ArrayList<>();
        QueryBuilder single = sqlWhere.setWhere(whereLogicals, sqlWhere.getWhereExpr());
        QueryBuilder boolQueryBuilder = sqlWhere.setWhereConnect(whereLogicals);
        if (whereLogicals.isEmpty() && single != null) {
            searchSourceBuilder.query(single);
            boolQueryBuilder = single;
        } else if (!whereLogicals.isEmpty()) {
            searchSourceBuilder.query(boolQueryBuilder);
        }

        // 取外部rank的限制数量
        SqlWhere parentSqlWhere = new SqlWhere(parentQueryBlock);
        int row_number = parentSqlWhere.getRankNum(row_number_alias);

        CollapseBuilder collapseBuilder = new CollapseBuilder(partitionBy);
        InnerHitBuilder innerHitBuilder = new InnerHitBuilder(partitionBy + "InnerHit");
        innerHitBuilder.setSize(row_number);
        subSqlSelect.setSelect(innerHitBuilder);
        sqlOrderBy.setOrder(innerHitBuilder);
        collapseBuilder.setInnerHits(innerHitBuilder);

        searchSourceBuilder.query(boolQueryBuilder)
                .collapse(collapseBuilder);

        List<EsGroupByTopNResult> esGroupResults = esDao.topNGroupByCollapse(tableName, searchSourceBuilder, result);
        logger.debug("测试:" + esGroupResults);

        return Collections.singletonList(esGroupResults);
    }


    private static String getTableName(SQLSelectStatement stmt) {
        // 获取表名，只支持简单的单表查询
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);
        Map<TableStat.Name, TableStat> tablesMap = visitor.getTables();
        String tableName = null;
        for (Map.Entry<TableStat.Name, TableStat> entry : tablesMap.entrySet()) {
            tableName = entry.getKey().getName();
//            System.out.println(tableName);
            if (tableName != null) {
                break;
            }
        }
        return tableName;
    }
}
