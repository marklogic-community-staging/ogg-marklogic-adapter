package oracle.goldengate.delivery.handler.marklogic.models;

import oracle.goldengate.datasource.meta.TableName;
import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;

import java.util.*;

/**
 * Created by prawal on 1/23/17.
 */
public class WriteListItem {

    private String uri;
    private String oldUri;
    private byte[] binary;

	private Map<String, Object> map = new HashMap<>();
    // allowed values UPDATE OR INSERT
    private String operation = null;
    private Collection<String> collection =  new ArrayList<String>();
    public static final String UPDATE = "update";
    public static final String INSERT = "insert";

    public WriteListItem(String uri, Map<String, Object> map, String operation) {
        this.uri = uri;
        this.map = map;
        this.operation = operation;
    }
    
    public WriteListItem(String uri, Map<String, Object> map, String operation, String collection) {
        this.uri = uri;
        this.map = map;
        this.operation = operation;
        this.collection.add(collection);
    }

    public WriteListItem(String uri, Map<String, Object> map, String operation, TableName table, HandlerProperties handlerProperties) {
        this.uri = uri;
        this.map = map;
        this.operation = operation;

        String org = handlerProperties.getOrg();
        String collectionPrefix = (org != null) ? "/" + org + "/" : "/";

        this.collection.add(collectionPrefix + table.getSchemaName().toLowerCase()  + "/" + table.getShortName().toLowerCase());
        this.collection.add(collectionPrefix + table.getSchemaName().toLowerCase());
    }
    
    public WriteListItem(String uri, byte[] binary, String operation, TableName table, HandlerProperties handlerProperties, String imageCollection) {
        this.uri = uri;
        this.map = null;
        this.binary = binary;
        this.operation = operation;

        String org = handlerProperties.getOrg();
        String collectionPrefix = (org != null) ? "/" + org + "/" : "/";

        this.collection.add(collectionPrefix + table.getSchemaName().toLowerCase()  + "/" + table.getShortName().toLowerCase());
        this.collection.add(collectionPrefix + table.getSchemaName().toLowerCase());
        this.collection.add(imageCollection);
    }


    public String getUri() {
        return this.uri;
    }

    public String getOldUri() {
        return this.oldUri;
    }

    public void setOldUri(String oldUri) {
        this.oldUri = oldUri;
    }

    public Map<String,Object> getMap() {
        return this.map;
    }

    public String getOperation() {
       return this.operation;
    }

    public Collection<String> getCollection() {
        return this.collection;
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
}
