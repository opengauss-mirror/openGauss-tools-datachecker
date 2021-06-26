package com.gauss.extractor;

import lombok.Getter;
import lombok.Setter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gauss.common.lifecycle.AbstractGaussLifeCycle;
import com.gauss.common.model.ExtractStatus;
import com.gauss.common.stats.ProgressTracer;

public abstract class AbstractRecordExtractor extends AbstractGaussLifeCycle implements RecordExtractor {

    protected final Logger           logger = LoggerFactory.getLogger(this.getClass());

    @Getter
    @Setter
    protected volatile ExtractStatus status = ExtractStatus.NORMAL;

    public ExtractStatus status() {
        return status;
    }

    @Getter
    @Setter
    protected volatile ProgressTracer tracer;
}
