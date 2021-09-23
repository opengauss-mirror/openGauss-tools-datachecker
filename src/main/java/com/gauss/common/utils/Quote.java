/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 */
package com.gauss.common.utils;

import com.gauss.common.model.DbType;

import java.util.Arrays;
import java.util.stream.Collectors;

public class Quote {
    private Quote() {}
    public static Quote ins = new Quote();
    private boolean needQuote = true;
    public void setSourceType(DbType typ) {
        if (typ == DbType.ORACLE) {
            needQuote = false;
        }
    }

    public String quote(String identifier) {
        if (needQuote) {
            return "\"" + identifier + "\"";
        } else {
            return identifier;
        }
    }

    public static String join(String delimiter, String ...args) {
        return Arrays.stream(args).collect(Collectors.joining(delimiter));
    }
}
