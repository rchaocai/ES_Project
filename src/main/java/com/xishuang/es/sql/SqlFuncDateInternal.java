package com.xishuang.es.sql;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntervalExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntervalUnit;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;

import java.util.List;

/**
 * 针对时间间隔分组，用到es中的{@link org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder}
 * 文档地址为{@see https://www.elastic.co/guide/en/elasticsearch/reference/7.x/search-aggregations-bucket-datehistogram-aggregation.html}
 * 函数定义为
 * DateInterval(utc_time, INTERVAL 1 second)
 * DateInterval(utc_time, INTERVAL 1 minute)
 * DateInterval(utc_time, INTERVAL 1 hour)
 * DateInterval(utc_time, INTERVAL 1 day)
 * DateInterval(utc_time, INTERVAL 1 week)
 */
public class SqlFuncDateInternal {
    private List<SQLSelectItem> methodList;
    private boolean isDateInternal;
    private String columnName;
    private SQLIntervalUnit unit;
    private int value;

    public SqlFuncDateInternal(SqlSelect sqlSelect) {
        initFunc(sqlSelect);
    }

    private void initFunc(SqlSelect sqlSelect) {
        List<SQLSelectItem> methodList = sqlSelect.getMethodList();
        this.methodList = methodList;
        for (SQLSelectItem item : methodList) {
            SQLMethodInvokeExpr expr = (SQLMethodInvokeExpr) item.getExpr();
            String methodName = expr.getMethodName();
            if (methodName.equals("dateinterval")) {
                this.isDateInternal = true;
                this.columnName = expr.getChildren().get(0).toString();
                SQLIntervalExpr intervalExpr = (SQLIntervalExpr) expr.getChildren().get(1);
                this.unit = intervalExpr.getUnit();
                this.value = Integer.parseInt(intervalExpr.getValue().toString());
            }
            SQLIntervalExpr intervalExpr = (SQLIntervalExpr) expr.getChildren().get(1);
        }
    }

    /**
     * mysql的时间间隔转换为es的时间间隔
     */
    public DateHistogramInterval getInternal() {
        if (unit == SQLIntervalUnit.SECOND) {
            if (value > 1) return DateHistogramInterval.seconds(value);
            return DateHistogramInterval.SECOND;
        } else if (unit == SQLIntervalUnit.MINUTE) {
            if (value > 1) return DateHistogramInterval.minutes(value);
            return DateHistogramInterval.MINUTE;
        } else if (unit == SQLIntervalUnit.HOUR) {
            if (value > 1) return DateHistogramInterval.hours(value);
            return DateHistogramInterval.HOUR;
        } else if (unit == SQLIntervalUnit.DAY) {
            if (value > 1) return DateHistogramInterval.days(value);
            return DateHistogramInterval.DAY;
        } else if (unit == SQLIntervalUnit.WEEK) {
            if (value > 1) return DateHistogramInterval.weeks(value);
            return DateHistogramInterval.WEEK;
        } else if (unit == SQLIntervalUnit.MONTH) {
            return DateHistogramInterval.MONTH;
        } else if (unit == SQLIntervalUnit.YEAR) {
            return DateHistogramInterval.YEAR;
        } else {
            throw new IllegalArgumentException("不支持时间单位：" + unit);
        }
    }

    public List<SQLSelectItem> getMethodList() {
        return methodList;
    }

    public boolean isDateInternal() {
        return isDateInternal;
    }

    public String getColumnName() {
        return columnName;
    }

    public SQLIntervalUnit getUnit() {
        return unit;
    }

    public int getValue() {
        return value;
    }
}
