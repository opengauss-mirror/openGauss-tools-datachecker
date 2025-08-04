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

package com.gauss.common.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.builder.ToStringStyle;

/**
 * 统一的ToStringStyle
 */
public class GaussToStringStyle extends ToStringStyle {

    private static final long         serialVersionUID = -6568177374288222145L;

    public static final ToStringStyle DEFAULT_STYLE    = new DateStyle("yyyy-MM-dd HH:mm:ss");

    private static class DateStyle extends ToStringStyle {

        private static final long serialVersionUID = 5208917932254652886L;

        private String            datePattern;

        public DateStyle(String datePattern){
            super();
            this.setUseIdentityHashCode(false);
            this.setUseShortClassName(true);
            this.datePattern = datePattern;
        }

        protected void appendDetail(StringBuffer buffer, String fieldName, Object value) {
            if (value instanceof Date) {
                value = new SimpleDateFormat(datePattern).format(value);
            } else {
                buffer.append(value);
            }
        }
    }
}
