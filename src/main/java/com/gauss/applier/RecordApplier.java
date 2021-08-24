package com.gauss.applier;

import java.util.List;

import com.gauss.common.lifecycle.GaussLifeCycle;
import com.gauss.exception.GaussException;

/**
 * copy checksum into target database
 */
public interface RecordApplier extends GaussLifeCycle {

    public void apply(List<String> records) throws GaussException;

}
