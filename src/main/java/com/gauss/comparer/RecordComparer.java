package com.gauss.comparer;

import com.gauss.common.lifecycle.GaussLifeCycle;
import com.gauss.common.model.CompareStatus;
import com.gauss.exception.GaussException;

/**
 * prepare checksum table in target database
 */
public interface RecordComparer extends GaussLifeCycle {
    public void compare() throws GaussException;
}
