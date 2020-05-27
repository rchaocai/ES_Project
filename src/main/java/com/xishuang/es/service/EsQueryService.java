package com.xishuang.es.service;

import com.xishuang.es.dao.EsDao;
import com.xishuang.es.domain.EsParam;
import com.xishuang.es.domain.Result;
import com.xishuang.es.sql.SqlParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class EsQueryService {
    @Autowired
    private EsDao esDao;

    public Result<List<? extends Object>> query(EsParam esParam) throws Exception {
        Result<List<? extends Object>> result = new Result<>();
        result.setDebug(esParam.isDebug());
        List<? extends Object> resultList = SqlParser.parse(esDao, esParam.getSql(), result);
        return result.setData(resultList);
    }
}
