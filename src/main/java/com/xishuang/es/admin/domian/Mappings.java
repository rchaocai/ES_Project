package com.xishuang.es.admin.domian;

import com.alibaba.fastjson.JSON;

import java.util.Map;

/**
 * {
 *   "kibana_sample_data_logs" : {
 *     "aliases" : { },
 *     "mappings" : {
 *       "properties" : {
 *         "agent" : {
 *           "type" : "text",
 *           "fields" : {
 *             "keyword" : {
 *               "type" : "keyword",
 *               "ignore_above" : 256
 *             }
 *           }
 *         },
 *         "bytes" : {
 *           "type" : "long"
 *         },
 *         "event" : {
 *           "properties" : {
 *             "dataset" : {
 *               "type" : "keyword"
 *             }
 *           }
 *         },
 *         "ip" : {
 *           "type" : "ip"
 *         }
 *
 *       }
 *     },
 *     "settings" : {
 *       "index" : {
 *         "number_of_shards" : "1",
 *         "auto_expand_replicas" : "0-1",
 *         "provided_name" : "kibana_sample_data_logs",
 *         "creation_date" : "1589703282630",
 *         "number_of_replicas" : "0",
 *         "uuid" : "rsnWTFICQoafqEK2pA9oVw",
 *         "version" : {
 *           "created" : "7070099"
 *         }
 *       }
 *     }
 *   }
 * }
 */
public class Mappings {
    private Map<String, Property> properties;

    public static class Property {
        private String type;
        /**
         * 嵌套的字段先用fields字段占着，这样是有问题的，没能解析出真正的字段
         */
        private Map<String, Object> fields;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Map<String, Object> getFields() {
            return fields;
        }

        public void setFields(Map<String, Object> fields) {
            this.fields = fields;
        }

        @Override
        public String toString() {
            return JSON.toJSONString(this);
        }
    }

    public Map<String, Property> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Property> properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
