package oracle.goldengate.delivery.handler.marklogic;

import com.marklogic.client.DatabaseClient;
import oracle.goldengate.delivery.handler.marklogic.models.DeleteList;
import oracle.goldengate.delivery.handler.marklogic.models.TruncateList;
import oracle.goldengate.delivery.handler.marklogic.models.WriteListItem;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class HandlerProperties {

    private DatabaseClient client;
    private String database;
    private String host;
    private String port;
    private String user;
    private String password;
    private String format = "json";
    private String auth = "digest";
    private String rootName;
    private String transformName;

    private boolean gateway = false;
    private boolean ssl = false;
    private String truststore = null;
    private String truststoreFormat = "PKCS12";
    private String truststorePassword = null;

    private int threadCount = 4;
    private int batchSize = 100;

    private HashMap<String, String> transformParams;
    private Collection<String> collections = new ArrayList<String>();

    private String org;
    private String schema;
    private String application;
    private String imageDb;
    private String imageProperty;
    private String imageFormat;
    private String imageCollection;
    private HashMap<String, String[]> imageKeyProps;
    private final String uriDelimiter = "/";

    public Long totalInserts = 0L;
    public Long totalBinaryInserts = 0L;
    public Long totalUpdates = 0L;
    public Long totalDeletes = 0L;
    public Long totalTruncates = 0L;
    public Long totalTxns = 0L;
    public Long totalOperations = 0L;

    public List<WriteListItem> writeList = new ArrayList<>();
    public DeleteList deleteList = new DeleteList();
    public TruncateList truncateList = new TruncateList();

    public DatabaseClient getClient() {
        return client;
    }

    public void setClient(DatabaseClient client) {
        this.client = client;
    }

    public boolean isGateway() {
        return gateway;
    }

    public void setGateway(boolean gateway) {
        this.gateway = gateway;
    }

    public boolean isSsl() {
        return ssl;
    }

    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }

    public String getTruststore() {
        return truststore;
    }

    public void setTruststore(String truststore) {
        this.truststore = truststore;
    }

    public String getTruststoreFormat() {
        return truststoreFormat;
    }

    public void setTruststoreFormat(String truststoreFormat) {
        this.truststoreFormat = truststoreFormat;
    }

    public String getTruststorePassword() {
        return truststorePassword;
    }

    public void setTruststorePassword(String truststorePassword) {
        this.truststorePassword = truststorePassword;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public void setAuth(String auth) {
        this.auth = auth;
    }

    public String getAuth() {
        return auth;
    }

    public void setRootName(String rootName) {
        this.rootName = rootName;
    }

    public String getRootName() {
        return rootName;
    }

    public void setTransformName(String transformName) {
        this.transformName = transformName;
    }

    public String getTransformName() {
        return transformName;
    }

    public void setTransformParams(String transformParams) {
        if (transformParams == null) {
            this.transformParams = null;
        } else {
            // entity=Article,flow=GkgCsv,flowType=input
            this.transformParams = new HashMap<>();

            String[] params = transformParams.split(",");
            for (String param : params) {
                String[] parts = param.split("=");
                this.transformParams.put(parts[0], parts[1]);
            }
        }
    }

    public HashMap<String, String> getTransformParams() {
        return transformParams;
    }

    /**
     * Pass in a comma-delimited list to set multiple collections
     */
    public void setCollections(String collections) {
        synchronized (this.collections) {
            this.collections.clear();

            if (collections != null) {
                String[] collectionList = collections.split(",");
                for (String collection : collectionList) {
                    this.collections.add(collection);
                }
            }
        }
    }

    public Collection<String> getCollections() {
        return this.collections;
    }

    public String getOrg() {
        return org;
    }

    public void setOrg(String org) {
        this.org = org;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getApplication() {
        return application;
    }

    public void setApplication(String application) {
        this.application = application;
    }

    public String getImageDb() {
        return imageDb;
    }

    public void setImageDb(String imageDb) {
        this.imageDb = imageDb;
    }

    public String getImageProperty() {
        return imageProperty;
    }

    public void setImageProperty(String imageProperty) {
        this.imageProperty = imageProperty;
    }

    public String getImageFormat() {
        return imageFormat;
    }

    public void setImageFormat(String imageFormat) {
        this.imageFormat = imageFormat;
    }

    public String getImageCollection() {
        return imageCollection;
    }

    public void setImageCollection(String imageCollection) {
        this.imageCollection = imageCollection;
    }

    public HashMap<String, String[]> getImageKeyProps() {
        return imageKeyProps;
    }

    public void setImageKeyProps(String imageKeyProps) {
        if (imageKeyProps == null) {
            this.imageKeyProps = null;
        } else {
            // schema.table,prop1,prop2
            if (this.imageKeyProps == null) {
                this.imageKeyProps = new HashMap<String, String[]>();
            }
            String[] props = imageKeyProps.split(":");
            this.imageKeyProps.put(props[0], props[1].split(","));
        }
    }

    public String getUriDelimiter() {
        return uriDelimiter;
    }

}
