package com.xishuang.es.controller;

import com.xishuang.es.domain.EsParam;
import com.xishuang.es.domain.Result;
import com.xishuang.es.service.EsQueryService;
import com.xishuang.es.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/es")
public class EsAdmin {

    @Autowired
    private EsQueryService esQueryService;

    @RequestMapping(value="/query",method= RequestMethod.POST)
    public Result listAreaCrash(@RequestBody EsParam esParam) {
        // 参数预处理
        if(StringUtils.isBlank(esParam.getSql())) {
            return Result.fail().setReturnMsg("sql不能为空");
        }

        try {
            return esQueryService.query(esParam);
        } catch (Exception e) {
            return Result.fail().setReturnMsg(e.getMessage());
        }
    }

}
