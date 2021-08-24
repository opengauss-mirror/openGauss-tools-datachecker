package com.gauss.preparer;

import lombok.Getter;
import lombok.Setter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gauss.common.lifecycle.AbstractGaussLifeCycle;
import com.gauss.common.model.PrepareStatus;
import com.gauss.common.stats.ProgressTracer;

public abstract class AbstractRecordPreparer extends AbstractGaussLifeCycle{
    protected final Logger           logger = LoggerFactory.getLogger(this.getClass());

    @Getter
    @Setter
    protected volatile PrepareStatus status = PrepareStatus.BEGIN;

    @Getter
    @Setter
    protected volatile ProgressTracer tracer;
}
