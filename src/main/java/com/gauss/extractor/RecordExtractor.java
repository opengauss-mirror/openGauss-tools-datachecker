package com.gauss.extractor;

import java.util.List;

import com.gauss.common.lifecycle.GaussLifeCycle;
import com.gauss.common.model.ExtractStatus;
import com.gauss.common.model.record.Record;
import com.gauss.exception.GaussException;

import lombok.Getter;
import lombok.Setter;

/**
 * extract data
 */
public interface RecordExtractor extends GaussLifeCycle {

    public List<String> extract() throws GaussException;
    public ExtractStatus getStatus();
}
