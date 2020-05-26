package com.xishuang.es.domain;

import com.alibaba.fastjson.JSON;

import java.util.List;

/**
 * 分组查询
 * 代表一个分组
 */
public class EsGroupResult {
    /**
     * 分组值
     */
    private String value;
    /**
     * 一个分组内匹配到的DO实体集合
     */
    private List<String> beans;
    /**
     * 一个分组内匹配到的总数
     */
    private long amounts;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public List<String> getBeans() {
        return beans;
    }

    public void setBeans(List<String> beans) {
        this.beans = beans;
    }

    public long getAmounts() {
        return amounts;
    }

    public void setAmounts(long amounts) {
        this.amounts = amounts;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
