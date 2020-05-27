package com.xishuang.es.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.xishuang.es.util.SqlUtil;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;

import java.util.ArrayList;
import java.util.List;

import static com.xishuang.es.util.SqlUtil.isColumnMatch;

public class SqlGroupBy {
    private final SQLSelectGroupByClause groupByClause;

    public SqlGroupBy(SQLSelectQueryBlock sqlSelectQueryBlock) {
        this.groupByClause = sqlSelectQueryBlock.getGroupBy();
    }

    public boolean isGroupBy() {
        return groupByClause != null;
    }

    /**
     * 是否是多字段分组
     */
    public boolean isMultiGroup() {
        if (!isGroupBy()) return false;
        List<SQLExpr> groupByList = groupByClause.getItems();
        return groupByList.size() > 1;
    }

    /**
     * 多字段聚合，group by语句进行处理
     */
    public List<CompositeValuesSourceBuilder<?>> setGroupByMultiField() {
        List<SQLExpr> groupByList = groupByClause.getItems();
        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
        for (SQLExpr sqlExpr : groupByList) {
            String column = sqlExpr.toString();
            sources.add(new TermsValuesSourceBuilder(column).field(column).missingBucket(true));
        }

        return sources;
    }

    /**
     * 单字段聚合，group by语句进行处理
     * select host.keyword, count() as count from kibana_sample_data_logs group by host having count() > 2807 order by count desc limit 2
     *
     * @param sqlSelect  select字段
     * @param sqlOrderBy order by处理
     * @param sqlLimit   limit处理
     */
    public AggregationBuilder setGroupBySingleField(SqlSelect sqlSelect, SqlOrderBy sqlOrderBy, SqlLimit sqlLimit) {
        // 获取聚合函数字段，having与order by需要对比
        List<SQLSelectItem> selectList = sqlSelect.getNoNormalColumnList();
        if (selectList.isEmpty()) {
            throw new IllegalArgumentException("聚合函数为空!");
        }
        // 先看看是否是要按时间间隔分组
        SqlFuncDateInternal funcDateInternal = new SqlFuncDateInternal(sqlSelect);
        boolean isDateInternal = funcDateInternal.isDateInternal();

        if (isDateInternal) {
            return getDateHistogramAggregationBuilder(sqlSelect, sqlOrderBy, funcDateInternal);
        } else {
            return getTermsAggregationBuilder(sqlSelect, sqlOrderBy, sqlLimit);
        }
    }

    /**
     * 按时间间隔分组的聚合查询
     */
    private DateHistogramAggregationBuilder getDateHistogramAggregationBuilder(SqlSelect sqlSelect, SqlOrderBy sqlOrderBy, SqlFuncDateInternal funcDateInternal) {
        // 获取聚合函数字段，having与order by需要对比
        List<SQLSelectItem> selectList = sqlSelect.getAggList();
        // 获取分组字段
        List<SQLExpr> groupByList = groupByClause.getItems();
        String groupByColumn = groupByList.get(0).toString();
        // 获取时间字段
        String timeColumn = funcDateInternal.getColumnName();
        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = AggregationBuilders
                .dateHistogram(groupByColumn)
                .field(timeColumn)
                .offset(0)
                .format("yyyy-MM-dd HH:mm:ss")
                .dateHistogramInterval(funcDateInternal.getInternal());
        // 简单对having做处理，只支持having>和having>=
        SQLExpr havingExpr = groupByClause.getHaving();
        if (havingExpr != null) {
            SQLBinaryOpExpr havingBinary = ((SQLBinaryOpExpr) havingExpr);
            SQLBinaryOperator operator = havingBinary.getOperator();
            SQLExpr rightExpr = havingBinary.getRight();
            SQLExpr leftExpr = havingBinary.getLeft();
            String rightExprStr = rightExpr.toString();
            String leftExprStr = leftExpr.toString();
            // 检查having字段是否匹配
            boolean isMatch = isColumnMatch(selectList, leftExprStr);
            if (!isMatch) {
                throw new IllegalArgumentException("having对应字段与聚合函数不匹配!" + leftExprStr);
            }
            switch (operator) {
                case GreaterThanOrEqual:
                    dateHistogramAggregationBuilder.minDocCount(Long.parseLong(rightExprStr));
                    break;
                case GreaterThan:
                    dateHistogramAggregationBuilder.minDocCount(Long.parseLong(rightExprStr) + 1);
                    break;
                default:
                    throw new IllegalArgumentException("暂不支持该运算符!" + operator.toString());
            }
        }
        // 对order进行处理
        sqlOrderBy.setOrder(dateHistogramAggregationBuilder, selectList, groupByColumn);
        // 不支持对limit进行处理

        return dateHistogramAggregationBuilder;
    }

    /**
     * 一般情况下的聚合查询
     */
    private TermsAggregationBuilder getTermsAggregationBuilder(SqlSelect sqlSelect, SqlOrderBy sqlOrderBy, SqlLimit sqlLimit) {
        // 获取聚合函数字段，having与order by需要对比
        List<SQLSelectItem> selectList = sqlSelect.getAggList();
        // 获取分组字段
        List<SQLExpr> groupByList = groupByClause.getItems();
        String groupByColumn = groupByList.get(0).toString();
        TermsAggregationBuilder termsAggregationBuilder;
        if (isMultiGroup()) {
            Script script = new Script(SqlUtil.combineGroupBy(groupByList, "#"));
            termsAggregationBuilder = AggregationBuilders.terms(groupByColumn).script(script);
        } else {
            termsAggregationBuilder = AggregationBuilders.terms(groupByColumn).field(groupByColumn);
        }

        // 聚合函数sum
//        SumAggregationBuilder sumAggregationBuilder = AggregationBuilders.sum("sum_bytes").field("bytes");
//        termsAggregationBuilder.subAggregation(sumAggregationBuilder);

        // 简单对having做处理，只支持having>和having>=
        SQLExpr havingExpr = groupByClause.getHaving();
        if (havingExpr != null) {
            SQLBinaryOpExpr havingBinary = ((SQLBinaryOpExpr) havingExpr);
            SQLBinaryOperator operator = havingBinary.getOperator();
            SQLExpr rightExpr = havingBinary.getRight();
            SQLExpr leftExpr = havingBinary.getLeft();
            String rightExprStr = rightExpr.toString();
            String leftExprStr = leftExpr.toString();
            // 检查having字段是否匹配
            boolean isMatch = isColumnMatch(selectList, leftExprStr);
            if (!isMatch) {
                throw new IllegalArgumentException("having对应字段与聚合函数不匹配!" + leftExprStr);
            }
            switch (operator) {
                case GreaterThanOrEqual:
                    termsAggregationBuilder.minDocCount(Long.parseLong(rightExprStr));
                    break;
                case GreaterThan:
                    termsAggregationBuilder.minDocCount(Long.parseLong(rightExprStr) + 1);
                    break;
                default:
                    throw new IllegalArgumentException("暂不支持该运算符!" + operator.toString());
            }
        }

        // 对order进行处理
        sqlOrderBy.setOrder(termsAggregationBuilder, selectList, groupByColumn);
        // 对limit进行处理
        sqlLimit.setLimit(termsAggregationBuilder);

        return termsAggregationBuilder;
    }

}
