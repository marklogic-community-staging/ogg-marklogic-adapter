package oracle.goldengate.delivery.handler.marklogic.models;

import oracle.goldengate.util.DateString;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by prawal on 1/23/17.
 */
public class WriteListItem {

    public enum OperationType {
        INSERT("insert"),
        PK_UPDATE("pk-update"),
        UPDATE("update"),
        DELETE("delete");

        private final String description;

        OperationType(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

    private String scn;
    private String uri;
    private String oldUri;
    private byte[] binary;
    private String sourceSchema;
    private String sourceTable;
    private DateString timestamp;

    private Map<String, Object> map = new HashMap<>();
    // allowed values UPDATE OR INSERT
    private String operation = null;
    private Collection<String> collection = new ArrayList<String>();
    public static final String UPDATE = "update";
    public static final String INSERT = "insert";

    public WriteListItem() {

    }

    public String getUri() {
        return this.uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getOldUri() {
        return this.oldUri;
    }

    public void setOldUri(String oldUri) {
        this.oldUri = oldUri;
    }

    public Map<String, Object> getMap() {
        return this.map;
    }

    public void setMap(Map<String, Object> map) {
        this.map = map;
    }

    public String getOperation() {
        return this.operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public Collection<String> getCollection() {
        return this.collection;
    }

    public void setCollection(Collection<String> collection) {
        this.collection = collection;
    }

    public byte[] getBinary() {
        return binary;
    }

    public void setBinary(byte[] binary) {
        this.binary = binary;
    }

    public boolean isBinary() {
        return (this.binary != null);
    }

    public String getSourceSchema() {
        return sourceSchema;
    }

    public void setSourceSchema(String sourceSchema) {
        this.sourceSchema = sourceSchema;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getScn() {
        return scn;
    }

    public void setScn(String scn) {
        this.scn = scn;
    }

    public DateString getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(DateString timestamp) {
        this.timestamp = timestamp;
    }
}
