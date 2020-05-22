package com.xishuang.es.sql;

import com.xishuang.es.util.StringUtils;

public class SqlQueryBuilder implements Cloneable {
    /**
     * select子句
     */
    private String select;
    
    /**
     * where子句
     */
    private String where;
    
    /**
     * 分组
     */
    private String groupBy;
    
    /**
     * having子句
     */
    private String having;
    
    /**
     * 排序
     */
    private String orderBy;
    
    /**
     * 偏移量
     */
    private Integer offset;
    
    /**
     * 数据量限制
     */
    private Integer limit;
    
    public String getSelect() {
        return select;
    }

    public SqlQueryBuilder setSelect(String select) {
        this.select = select;
        return this;
    }
    
    public String getWhere() {
        return where;
    }

    public SqlQueryBuilder setWhere(String where) {
        this.where = where;
        return this;
    }

    public String getGroupBy() {
        return groupBy;
    }

    public SqlQueryBuilder setGroupBy(String groupBy) {
        this.groupBy = groupBy;
        return this;
    }

    public String getHaving() {
        return having;
    }
    
    public SqlQueryBuilder setHaving(String having) {
        this.having = having;
        return this;
    }

    public String getOrderBy() {
        return orderBy;
    }
    
    public SqlQueryBuilder setOrderBy(String orderBy) {
        this.orderBy = orderBy;
        return this;
    }
    
    public Integer getOffset() {
        return offset;
    }
    
    public SqlQueryBuilder setOffset(Integer offset) {
        this.offset = offset;
        return this;
    }
    
    public Integer getLimit() {
        return limit;
    }
    
    public SqlQueryBuilder setLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public String formatParam(String database, String measurement) {
        StringBuilder strBuilder = new StringBuilder("");
        if (StringUtils.isNotBlank(select)) {
            strBuilder.append("select ").append(select);
        } else {
            strBuilder.append("select * ");
        }
        strBuilder.append(" from ");
        if (StringUtils.isNotBlank(database)) {
            strBuilder.append(database).append(".");
        }
        strBuilder.append(measurement);
        if (StringUtils.isNotBlank(where)) {
            strBuilder.append(" where ").append(where);
        }
        if (StringUtils.isNotBlank(groupBy)) {
            strBuilder.append(" group by ").append(groupBy);
        }
        if (StringUtils.isNotBlank(having)) {
            strBuilder.append(" having ").append(having);
        }
        if (StringUtils.isNotBlank(orderBy)) {
            strBuilder.append(" order by ").append(orderBy);
        }
        if (limit != null && limit >= 0) {
            strBuilder.append(" limit ");
            if (offset != null && offset >= 0) {
                strBuilder.append(offset).append(", ");
            }
            strBuilder.append(limit);
        }
        return strBuilder.toString();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("SqlQueryParam [");
        if (select != null)
            builder.append("select=").append(select).append(", ");
        if (where != null)
            builder.append("where=").append(where).append(", ");
        if (groupBy != null)
            builder.append("groupBy=").append(groupBy).append(", ");
        if (having != null)
            builder.append("having=").append(having).append(", ");
        if (orderBy != null)
            builder.append("orderBy=").append(orderBy).append(", ");
        if (offset != null)
            builder.append("offset=").append(offset).append(", ");
        if (limit != null)
            builder.append("limit=").append(limit).append(", ");
        builder.append("]");
        return builder.toString();
    }
    
    @Override
    public Object clone() throws CloneNotSupportedException {
    	return super.clone();
    }
}
