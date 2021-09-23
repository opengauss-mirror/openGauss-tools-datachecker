/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 */
package com.gauss.common.db.sql;

import com.gauss.common.db.meta.Table;
import com.gauss.common.model.GaussContext;
import com.gauss.common.utils.Quote;

public class OpenGaussForMysql extends OpenGaussUtil {

    public OpenGaussForMysql(GaussContext context) {
        super(context);
        Table tableMeta = context.getTableMeta();
        String srcCompareTableName = Quote.join("", Quote.ins.quote(tableMeta.getSchema()), ".",
                Quote.ins.quote(tableMeta.getName() + "_dataCheckerA"));
        setSrcCompareTableName(srcCompareTableName);
        String destCompareTableName = Quote.join("", Quote.ins.quote(tableMeta.getSchema()), ".",
                Quote.ins.quote(tableMeta.getName() + "_dataCheckerB"));
        setDestCompareTableName(destCompareTableName);
        setConcatEnd(")");
        setConcatStart("concat_ws('',");
        setDelimiter(",");
        String originTableName = Quote.join("", Quote.ins.quote(tableMeta.getSchema()), ".",
                Quote.ins.quote(tableMeta.getName()));
        setOrinTableName(originTableName);
        setQuote("\"");
    }
}
