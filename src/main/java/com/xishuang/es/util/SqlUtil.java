package com.xishuang.es.util;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;

import java.util.List;

public class SqlUtil {

    /**
     * 字段是否匹配
     */
    public static boolean isColumnMatch(List<SQLSelectItem> selectList, String columnName) {
        for (SQLSelectItem selectItem : selectList) {
            String selectColumnName = selectItem.getExpr().toString();
            String alias = selectItem.getAlias();

            boolean isMatch = columnName.equals(selectColumnName) || columnName.equals(alias);
            if (isMatch) return true;
        }
        return false;
    }

    /**
     * 拼接分组字段
     */
    public static String combineGroupBy(List<SQLExpr> groupByList, String separator) {
        StringBuilder builder = new StringBuilder();
        separator = "+'" + separator + "'+";
        for (SQLExpr expr : groupByList) {
            builder.append("doc['").append(expr.toString()).append("'].value");
            builder.append(separator);
        }
        if (builder.length() != 0) {
            builder.delete(builder.length() - separator.length(), builder.length());
        }

        return builder.toString();
    }

    /**
     * 拼接分组字段
     */
    public static String combineGroupBy2(List<String> groupByList, String separator) {
        StringBuilder builder = new StringBuilder();
        separator = "+'" + separator + "'+";
        for (String expr : groupByList) {
            builder.append("doc['").append(expr.toString()).append("'].value");
            builder.append(separator);
        }
        if (builder.length() != 0) {
            builder.delete(builder.length() - separator.length(), builder.length());
        }

        return builder.toString();
    }

}
