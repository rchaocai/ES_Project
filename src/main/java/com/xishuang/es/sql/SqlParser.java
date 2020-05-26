package com.xishuang.es.sql;

import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;
import com.xishuang.es.dao.EsDao;
import com.xishuang.es.domain.EsAggregationResult;
import com.xishuang.es.domain.EsCommonResult;
import com.xishuang.es.domain.EsGroupResult;
import com.xishuang.es.domain.WhereLogical;
import com.xishuang.es.util.StringUtils;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
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


        SQLTableSource sqlTableSource = sqlSelectQueryBlock.getFrom();
        if (sqlTableSource instanceof SQLSubqueryTableSource) { // 是否存在子查询，主要针对row_number函数进行处理
            SQLSubqueryTableSource sqlSubqueryTableSource = (SQLSubqueryTableSource) sqlTableSource;
            SQLSelectQuery subSqlSelectQuery = sqlSubqueryTableSource.getSelect().getQuery();
            SQLSelectQueryBlock subSqlSelectQueryBlock = (SQLSelectQueryBlock) subSqlSelectQuery;
            parseSubQuery(esDao, subSqlSelectQueryBlock, sqlSelectQueryBlock);
        } else { // 正常的查询
            System.out.println(sqlSelectQueryBlock.getFrom());
            parse(esDao, sqlSelectQueryBlock);
        }
    }

    public static void parse(EsDao esDao, SQLSelectQueryBlock sqlSelectQueryBlock) throws Exception {
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
            EsCommonResult result = esDao.listToQueryResult(tableName, searchSourceBuilder);
            System.out.println("测试:" + result.getBeans().size());
            System.out.println("测试:" + result.getBeans());
        } else { // 分组聚合
            if (sqlGroupBy.isMultiGroup()) { // 多字段分组
                List<CompositeValuesSourceBuilder<?>> sources = sqlGroupBy.setGroupByMultiField();
                List<EsAggregationResult> list = esDao.groupByNoSort(tableName, boolQueryBuilder, sources);
                System.out.println("测试:" + list);
            } else { // 单字段分组
                TermsAggregationBuilder termsAggregationBuilder = sqlGroupBy.setGroupBySingleField(sqlSelect, sqlOrderBy, sqlLimit);
                List<EsAggregationResult> list = esDao.aggregateToQueryResult(tableName, boolQueryBuilder, termsAggregationBuilder);
                System.out.println("测试:" + list);
            }
        }
    }

    public static void parseSubQuery(EsDao esDao, SQLSelectQueryBlock subQueryBlock, SQLSelectQueryBlock parentQueryBlock) throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 获取表名
        String tableName = subQueryBlock.getFrom().toString();
        System.out.println(tableName);
        // 针对select字段做处理
        SqlSelect sqlSelect = new SqlSelect(subQueryBlock);
        sqlSelect.setSelect(searchSourceBuilder);
        SQLSelectItem selectItem = sqlSelect.getAggList().get(0);
        SQLAggregateExpr aggregateExpr = (SQLAggregateExpr) selectItem.getExpr();
        // 目前只支持对row_number函数做处理
        if (!"row_number".equals(aggregateExpr.getMethodName())) {
            throw new IllegalArgumentException("只支持对row_number函数做处理");
        }
        SqlRowNumber sqlRowNumber = new SqlRowNumber(selectItem);
        String row_number_alias = sqlRowNumber.getAlias();
        String partitionBy = sqlRowNumber.getPartitionBy();
        SqlOrderBy sqlOrderBy = new SqlOrderBy(sqlRowNumber.getOrderBy());
        // 针对limit做处理
        SqlLimit sqlLimit = new SqlLimit(subQueryBlock);
        int rowCount = sqlLimit.getRowCount();
        System.out.println(searchSourceBuilder.toString());
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

        TermsAggregationBuilder termsAggregate = AggregationBuilders.terms(partitionBy)
                .field(partitionBy)
                .size(rowCount); // 限制分组的数量
        TopHitsAggregationBuilder topHitsAggregateBuilder = AggregationBuilders.topHits("top").size(row_number);
        sqlOrderBy.setOrder(topHitsAggregateBuilder);

        List<EsGroupResult> esGroupResults = esDao.aggregateGroupPage(tableName, boolQueryBuilder, termsAggregate, topHitsAggregateBuilder);
        System.out.println("测试:" + esGroupResults);
    }

    private static String getTableName(SQLSelectStatement stmt) {
        // 获取表名，只支持简单的单表查询
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);
        Map<TableStat.Name, TableStat> tablesMap = visitor.getTables();
        System.out.println(visitor.getAggregateFunctions());
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
