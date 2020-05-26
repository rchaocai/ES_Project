package com.xishuang.es.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;

import java.util.ArrayList;
import java.util.List;

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
    public TermsAggregationBuilder setGroupBySingleField(SqlSelect sqlSelect, SqlOrderBy sqlOrderBy, SqlLimit sqlLimit) {
        // 获取聚合函数字段，having与order by需要对比
        List<SQLSelectItem> selectList = sqlSelect.getAggList();
        if (selectList.isEmpty()) {
            throw new IllegalArgumentException("聚合函数为空!");
        }

        // 获取分组字段
        List<SQLExpr> groupByList = groupByClause.getItems();
        String column = groupByList.get(0).toString();
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms(column).field(column);

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

        // 对order进行处理，暂时只处理一个
        if (sqlOrderBy.isOrderBy()) {
            List<SQLSelectOrderByItem> orderByItemList = sqlOrderBy.getOrderList();
            SQLSelectOrderByItem item = orderByItemList.get(0);
            boolean isMatch = isColumnMatch(selectList, item.getExpr().toString());
            if (!isMatch) {
                throw new IllegalArgumentException("order by对应字段与聚合函数不匹配!" + item.getExpr().toString());
            }
            termsAggregationBuilder.order(BucketOrder.count(sqlOrderBy.isASC(item)));
        }

        // 对limit进行处理
        if (sqlLimit.isLimit()) {
            termsAggregationBuilder.size(sqlLimit.getRowCount());
        }

        return termsAggregationBuilder;
    }

    /**
     * 字段是否匹配
     */
    private boolean isColumnMatch(List<SQLSelectItem> selectList, String columnName) {
        for (SQLSelectItem selectItem : selectList) {
            String selectColumnName = selectItem.getExpr().toString();
            String alias = selectItem.getAlias();
            System.out.println(selectColumnName);
            System.out.println(alias);
            System.out.println(columnName);

            boolean isMatch = columnName.equals(selectColumnName) || columnName.equals(alias);
            if (isMatch) return true;
        }
        return false;
    }

}
