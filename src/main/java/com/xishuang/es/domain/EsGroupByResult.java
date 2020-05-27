package com.xishuang.es.domain;

import com.alibaba.fastjson.JSON;

public class EsGroupByResult {
    private String key;
    private long value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
