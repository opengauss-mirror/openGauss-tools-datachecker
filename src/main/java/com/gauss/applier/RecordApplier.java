package com.gauss.applier;

import java.util.List;

import com.gauss.common.lifecycle.GaussLifeCycle;
import com.gauss.common.model.record.Record;
import com.gauss.exception.GaussException;

/**
 * 数据提交
 */
public interface RecordApplier extends GaussLifeCycle {

    public void apply(List<Record> records) throws GaussException;

}
