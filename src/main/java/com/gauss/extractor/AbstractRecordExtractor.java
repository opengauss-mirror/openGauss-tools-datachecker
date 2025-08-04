/*
This program is free software;
you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program;
if not, write to the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
*/

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

    @Getter
    @Setter
    protected volatile ProgressTracer tracer;
}
