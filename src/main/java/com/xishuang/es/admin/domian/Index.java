package com.xishuang.es.admin.domian;

import com.alibaba.fastjson.JSON;

/**
 * 索引相关信息
 */
public class Index {
    /**
     * s索引名称
     */
    private String index_name;
    /**
     * 分片数
     */
    private String number_of_shards;
    /**
     * 副本数
     */
    private String number_of_replicas;

    private String create_date;

    public String getIndex_name() {
        return index_name;
    }

    public void setIndex_name(String index_name) {
        this.index_name = index_name;
    }

    public String getNumber_of_shards() {
        return number_of_shards;
    }

    public void setNumber_of_shards(String number_of_shards) {
        this.number_of_shards = number_of_shards;
    }

    public String getNumber_of_replicas() {
        return number_of_replicas;
    }

    public void setNumber_of_replicas(String number_of_replicas) {
        this.number_of_replicas = number_of_replicas;
    }

    public String getCreate_date() {
        return create_date;
    }

    public void setCreate_date(String create_date) {
        this.create_date = create_date;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
