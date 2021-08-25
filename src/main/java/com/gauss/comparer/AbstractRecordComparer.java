package com.gauss.comparer;

import lombok.Getter;
import lombok.Setter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.gauss.common.lifecycle.AbstractGaussLifeCycle;
import com.gauss.common.model.CompareStatus;

public abstract class AbstractRecordComparer extends AbstractGaussLifeCycle implements RecordComparer {
    protected final Logger           logger = LoggerFactory.getLogger(this.getClass());

    @Getter
    @Setter
    protected volatile CompareStatus status = CompareStatus.BEGIN;
}
