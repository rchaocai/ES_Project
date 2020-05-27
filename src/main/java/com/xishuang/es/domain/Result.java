package com.xishuang.es.domain;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class Result<T> {
    /**
     * 0成功，1失败
     */
    private int returnCode;
    private String returnMsg;
    private T data;
    private boolean isDebug;
    private JSONObject search;

    public Result() {
    }

    public Result(int returnCode) {
        this.returnCode = returnCode;
    }

    public static Result success() {
        return new Result(0).setReturnMsg("success");
    }

    public static Result fail() {
        return new Result(1);
    }

    public int getReturnCode() {
        return returnCode;
    }

    public Result<T> setReturnCode(int returnCode) {
        this.returnCode = returnCode;
        return this;
    }

    public String getReturnMsg() {
        return returnMsg;
    }

    public Result<T> setReturnMsg(String returnMsg) {
        this.returnMsg = returnMsg;
        return this;
    }

    public T getData() {
        return data;
    }

    public Result<T> setData(T data) {
        this.data = data;
        return this;
    }

    public boolean isDebug() {
        return isDebug;
    }

    public void setDebug(boolean debug) {
        isDebug = debug;
    }

    public JSONObject getSearch() {
        return search;
    }

    public void setSearch(JSONObject search) {
        this.search = search;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
