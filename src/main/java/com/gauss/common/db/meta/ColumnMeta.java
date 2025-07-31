/*
This program is free software:
you can redistribute it and/or modify it under the terms of the GNU General Public License as published
by the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program.
If not, see <https://www.gnu.org/licenses/>.
*/

package com.gauss.common.db.meta;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.gauss.common.utils.GaussToStringStyle;

/**
 * represent the meta information of one column
 */
public class ColumnMeta {

    private String name;

    private int type;

    private String typeName = "";

    public ColumnMeta(String columnName, int columnType, String typeName) {
        this.name = columnName;
        this.type = columnType;
        this.typeName = typeName;
    }

    public String getTypeName() {
        return typeName;
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }

    public ColumnMeta clone() {
        return new ColumnMeta(this.name, this.type, this.typeName);
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, GaussToStringStyle.DEFAULT_STYLE);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + type;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ColumnMeta other = (ColumnMeta) obj;
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        if (type != other.type) {
            return false;
        }
        return true;
    }

}
