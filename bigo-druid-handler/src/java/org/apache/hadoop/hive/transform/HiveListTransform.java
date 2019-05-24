package org.apache.hadoop.hive.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.Row;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.transform.RowFunction;
import org.apache.druid.segment.transform.Transform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public class HiveListTransform implements Transform
{
    private final String name;
    private final String fieldName;
    public static final Logger LOG = LoggerFactory.getLogger(HiveListTransform.class);

    @JsonCreator
    public HiveListTransform(
            @JsonProperty("name") final String name,
            @JsonProperty("fieldName") final String fieldName
    )
    {
        this.name = Preconditions.checkNotNull(name, "name");
        this.fieldName = Preconditions.checkNotNull(fieldName, "fieldName");
    }

    @JsonProperty
    @Override
    public String getName() {
        return name;
    }

    @JsonProperty
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public RowFunction getRowFunction() {
        return new ListRowFunction(fieldName);
    }

    static class ListRowFunction implements RowFunction {
        private final String fieldName;

        public ListRowFunction(String fieldName) {
           this.fieldName = fieldName;
        }

        @Override
        public Object eval(Row row) {
            Object filedValue = row.getRaw(fieldName);
            if (filedValue instanceof List) {
                return filedValue;
            }
            return filedValue;
        }
    }

    private static String listAsString(List<String> list) {
        return String.join(",", list);
    }

    private static Object getValueFromRow(final Row row, final String column)
    {
        if (column.equals(ColumnHolder.TIME_COLUMN_NAME)) {
            return row.getTimestampFromEpoch();
        } else {
            return row.getRaw(column);
        }
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final HiveListTransform that = (HiveListTransform) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(fieldName, that.fieldName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, fieldName);
    }

    @Override
    public String toString()
    {
        return "HiveListTransform{" +
                "name='" + name + '\'' +
                ", fieldName='" + fieldName + '\'' +
                '}';
    }
}
