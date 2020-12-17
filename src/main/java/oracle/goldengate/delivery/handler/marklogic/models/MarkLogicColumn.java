package oracle.goldengate.delivery.handler.marklogic.models;

import java.util.Optional;

public class MarkLogicColumn {
    protected String name;
    protected Optional<Object> before;
    protected Optional<Object> after;
    protected boolean binary;

    public static final Object NULL = new Object();
    public static final Optional<Object> NULL_COLUMN_VALUE = Optional.of(NULL);

    public static MarkLogicColumn of(String name, boolean binary, Optional<Object> before, Optional<Object> after) {
        MarkLogicColumn markLogicColumn = new MarkLogicColumn();

        markLogicColumn.before = before;
        markLogicColumn.after = after;
        markLogicColumn.binary = binary;
        markLogicColumn.name = name;

        return markLogicColumn;
    }

    public String getName() {
        return name;
    }

    public Optional<Object> getBefore() {
        return before;
    }

    public Optional<Object> getAfter() {
        return after;
    }

    public boolean isBinary() {
        return binary;
    }
}
