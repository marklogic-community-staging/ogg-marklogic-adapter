package oracle.goldengate.delivery.handler.marklogic.models;

import java.util.*;

public class MarkLogicOp {
    protected SortedSet<String> keyColumns = new TreeSet<>();
    protected Set<String> binaryColumns = new HashSet<>();
    protected String table;
    protected String schema;

    protected Optional<Map<String, Object>> beforeValues = Optional.empty();
    protected Optional<Map<String, Object>> afterValues = Optional.empty();

    public MarkLogicOp withTable(String table) {
        this.table = table;
        return this;
    }

    public MarkLogicOp withSchema(String schema) {
        this.schema = schema;
        return this;
    }

    public MarkLogicOp withKeyColumn(String columnName) {
        keyColumns.add(columnName);
        return this;
    }

    public MarkLogicOp withBinaryColumn(String columnName) {
        binaryColumns.add(columnName);
        return this;
    }

    public boolean isBinary(String column) {
        return binaryColumns.contains(column);
    }

    protected MarkLogicOp withValue(String columnName, boolean useBefore, Object value) {
        Optional<Map<String, Object>> values;
        if(useBefore) {
            if(!this.beforeValues.isPresent()) {
                this.beforeValues = Optional.of(new HashMap<>());
            }
            values = this.beforeValues;
        } else {
            if(!this.afterValues.isPresent()) {
                this.afterValues = Optional.of(new HashMap<>());
            }
            values = this.afterValues;
        }

        values.ifPresent(bvMap -> bvMap.put(columnName, value));
        return this;
    }

    public MarkLogicOp withBeforeValue(String columnName, Object value) {
        return this.withValue(columnName, true, value);
    }

    public MarkLogicOp withAfterValue(String columnName, Object value) {
        return this.withValue(columnName, false, value);
    }

    public Optional<Map<String, Object>> getAfterValues() {
        return afterValues;
    }

    public Optional<Map<String, Object>> getBeforeValues() {
        return beforeValues;
    }

    public SortedSet<String> getKeyColumns() {
        return keyColumns;
    }

    public String getTable() {
        return table;
    }

    public String getSchema() {
        return schema;
    }
}
