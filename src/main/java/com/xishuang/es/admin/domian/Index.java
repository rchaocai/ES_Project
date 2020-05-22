package com.xishuang.es.admin.domian;

import com.alibaba.fastjson.JSON;
import com.xishuang.es.util.DateUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * 索引相关信息
 */
public class Index {
    /**
     * 索引名称
     */
    private String index_name;

    private Map<String, String> settings = new HashMap<>();
    private Mappings mappings = new Mappings();

    public String getIndex_name() {
        return index_name;
    }

    public void setIndex_name(String index_name) {
        this.index_name = index_name;
    }

    /**
     * 分片数
     */
    public int getNumber_of_shards() {
        return Integer.parseInt(settings.get("index.number_of_shards"));
    }

    public void setNumber_of_shards(int number_of_shards) {
        settings.put("index.number_of_shards", String.valueOf(number_of_shards));
    }

    /**
     * 副本数
     */
    public int getNumber_of_replicas() {
        return Integer.parseInt(settings.get("index.number_of_replicas"));
    }

    public void setNumber_of_replicas(int number_of_replicas) {
        settings.put("index.number_of_replicas", String.valueOf(number_of_replicas));
    }

    public String getCreate_date() {
        return DateUtil.millStr2DateStr(settings.get("index.creation_date"));
    }

    public Map<String, String> getSettings() {
        return settings;
    }

    public void setSettings(Map<String, String> settings) {
        this.settings = settings;
    }

    public Mappings getMappings() {
        return mappings;
    }

    public void setMappings(Mappings mappings) {
        this.mappings = mappings;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
