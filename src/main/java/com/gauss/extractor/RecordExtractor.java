package com.gauss.extractor;

import java.util.List;

import com.gauss.common.lifecycle.GaussLifeCycle;
import com.gauss.common.model.ExtractStatus;
import com.gauss.common.model.record.Record;
import com.gauss.exception.GaussException;

/**
 * 数据获取
 */
public interface RecordExtractor extends GaussLifeCycle {

    public List<Record> extract() throws GaussException;

    /**
     * @return 当前extractor的状态,{@linkplain ExtractStatus}
     */
    public ExtractStatus status();

}
