package com.xishuang.es.domain;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.common.Nullable;

import java.util.List;

public class EsCommonResult {
    /**
     * 查询匹配的总数，常用于在分页查询中计算总页数
     */
    private long amounts;
    /**
     * 查询得到的DO实体集合
     */
    private List<String> beans;

    /**
     * 如果没有进行排序，则该值为空
     * 如果设置排序，该值为最后一个hit的sortValues
     * 用于searchAfter查询
     */
    private @Nullable
    Object[] lastSortValues;

    public long getAmounts() {
        return amounts;
    }

    public void setAmounts(long amounts) {
        this.amounts = amounts;
    }

    public List<String> getBeans() {
        return beans;
    }

    public void setBeans(List<String> beans) {
        this.beans = beans;
    }

    public Object[] getLastSortValues() {
        return lastSortValues;
    }

    public void setLastSortValues(Object[] lastSortValues) {
        this.lastSortValues = lastSortValues;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
