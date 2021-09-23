/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 */
package com.gauss.common.db.sql;

import com.gauss.common.db.meta.Table;
import com.gauss.common.model.GaussContext;
import com.gauss.common.utils.Quote;

public class OpenGaussForOracle extends OpenGaussUtil {

    public OpenGaussForOracle(GaussContext context) {
        super(context);
        Table tableMeta = context.getTableMeta();
        String srcCompareTableName = Quote.join("", tableMeta.getSchema(), ".",
                tableMeta.getName(), "_dataCheckerA");
        setSrcCompareTableName(srcCompareTableName);
        String destCompareTableName = Quote.join("", tableMeta.getSchema(), ".",
                tableMeta.getName(), "_dataCheckerB");
        setDestCompareTableName(destCompareTableName);
        setConcatEnd("");
        setConcatStart("");
        setDelimiter(" || ");
        setOrinTableName(tableMeta.getSchema() + "." + tableMeta.getName());
        setQuote("");
    }
}
