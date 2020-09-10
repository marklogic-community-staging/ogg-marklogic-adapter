package oracle.goldengate.delivery.handler.marklogic;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import oracle.goldengate.datasource.*;
import oracle.goldengate.datasource.GGDataSource.Status;
import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.meta.DsMetaData;
import oracle.goldengate.datasource.meta.TableMetaData;
import oracle.goldengate.delivery.handler.marklogic.models.WriteListProcessor;
import oracle.goldengate.delivery.handler.marklogic.operations.OperationHandler;
import oracle.goldengate.delivery.handler.marklogic.util.DBOperationFactory;
import oracle.goldengate.delivery.handler.marklogic.util.URLFactory;
import oracle.goldengate.util.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;

public class MarkLogicHandler extends AbstractHandler {

    final private static Logger logger = LoggerFactory.getLogger(MarkLogicHandler.class);

    private HandlerProperties handlerProperties;

    private DBOperationFactory dbOperationFactory;

    public MarkLogicHandler() {
        super();
        handlerProperties = new HandlerProperties();
        dbOperationFactory = new DBOperationFactory();
    }

    @Override
    public void init(DsConfiguration arg0, DsMetaData arg1) {
        super.init(arg0, arg1);

        dbOperationFactory.init(handlerProperties);

        try {
            initMarkLogicClient();
        } catch (Exception e) {
            logger.error("Unable to connect to marklogic instance. Configured Server address list " + handlerProperties.getHost(), e);
            throw new ConfigException("Unable to connect to marklogic instance. Configured Server address list ", e);
        }
    }

    @Override
    public Status operationAdded(DsEvent e, DsTransaction tx, DsOperation dsOperation) {

        Status status = super.operationAdded(e, tx, dsOperation);

        Op op = new Op(dsOperation, getMetaData().getTableMetaData(dsOperation.getTableName()), getConfig());
        TableMetaData tableMetaData = getMetaData().getTableMetaData(op.getTableName());

        /**
         * Get the instance of incoming operation type from DBOperationFactory
         * */
        OperationHandler operationHandler = dbOperationFactory.getInstance(dsOperation.getOperationType());
        if (operationHandler != null) {
            try {
                operationHandler.process(tableMetaData, op);

                /** Increment the total number of operations */
                handlerProperties.totalOperations++;
            } catch (Exception e1) {
                status = Status.ABEND;
                logger.error("Unable to process operation.", e1);
            }
        } else {
            status = Status.ABEND;
            logger.error("Unable to instantiate operation handler for " + dsOperation.getOperationType().toString());
        }

        return status;
    }

    @Override
    public Status transactionBegin(DsEvent e, DsTransaction tx) {
        return super.transactionBegin(e, tx);
    }

    @Override
    public Status transactionCommit(DsEvent e, DsTransaction tx) {
        Status status = super.transactionCommit(e, tx);

        try {
            new WriteListProcessor(handlerProperties).process(handlerProperties.writeList);
            handlerProperties.deleteList.commit(handlerProperties);
            handlerProperties.truncateList.commit(handlerProperties);

            handlerProperties.writeList.clear();
            handlerProperties.deleteList.clear();
            handlerProperties.truncateList.clear();
        } catch (Exception ex) {
            logger.error("Error flushing records ", ex);
            status = Status.ABEND;
        }

        /**TODO: Add steps for rollback */

        handlerProperties.totalTxns++;

        return status;
    }

    @Override
    public String reportStatus() {
        StringBuilder sb = new StringBuilder();
        sb.append(":- Status report: mode=").append(getMode());
        sb.append(", transactions=").append(handlerProperties.totalTxns);
        sb.append(", operations=").append(handlerProperties.totalOperations);
        sb.append(", inserts=").append(handlerProperties.totalInserts);
        sb.append(", updates=").append(handlerProperties.totalUpdates);
        sb.append(", deletes=").append(handlerProperties.totalDeletes);
        sb.append(", truncates=").append(handlerProperties.totalTruncates);
        sb.append(", binaries=").append(handlerProperties.totalBinaryInserts);
        return sb.toString();
    }

    @Override
    public void destroy() {
        handlerProperties.getClient().release();
        super.destroy();
    }

    public X509TrustManager getDefaultTrustManager() throws NoSuchAlgorithmException, KeyStoreException {
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init((KeyStore) null);
        for (TrustManager trustManager : trustManagerFactory.getTrustManagers()) {
            if(trustManager instanceof X509TrustManager) {
                return (X509TrustManager) trustManager;
            }
        }
        return null;
    }

    public X509TrustManager getTrustManager(String truststoreLocation, String truststoreFormat, String truststorePassword) throws NoSuchAlgorithmException, KeyStoreException, IOException, CertificateException {
        KeyStore truststore = KeyStore.getInstance(truststoreFormat);

        try(InputStream truststoreInput = URLFactory.newURL(truststoreLocation).openStream()) {
            truststore.load(truststoreInput, truststorePassword.toCharArray());
        }

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(truststore);

        for (TrustManager trustManager : trustManagerFactory.getTrustManagers()) {
            if(trustManager instanceof X509TrustManager) {
                return (X509TrustManager) trustManager;
            }
        }

        return null;
    }

    private void initMarkLogicClient() throws Exception {
        DatabaseClientFactory.SecurityContext markLogicSecurityContext = handlerProperties.getAuth().equalsIgnoreCase("DIGEST") ?
            new DatabaseClientFactory.DigestAuthContext(handlerProperties.getUser(), handlerProperties.getPassword()) :
            new DatabaseClientFactory.BasicAuthContext(handlerProperties.getUser(), handlerProperties.getPassword());

        if(handlerProperties.isSsl()) {
            X509TrustManager trustManager = (handlerProperties.getTruststore() == null) ?
                        getDefaultTrustManager() :
                        getTrustManager(handlerProperties.getTruststore(), handlerProperties.getTruststoreFormat(), handlerProperties.getTruststorePassword());
            TrustManager[] trustManagers = { trustManager };

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustManagers, new SecureRandom());

            markLogicSecurityContext = markLogicSecurityContext
                .withSSLContext(sslContext, trustManager)
                .withSSLHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.COMMON);
        }

        DatabaseClient client = DatabaseClientFactory.newClient(
            handlerProperties.getHost(),
            Integer.parseInt(handlerProperties.getPort()),
            handlerProperties.getDatabase(),
            markLogicSecurityContext,
            handlerProperties.isGateway() ? DatabaseClient.ConnectionType.GATEWAY : DatabaseClient.ConnectionType.DIRECT
        );

        this.handlerProperties.setClient(client);
    }

    public void setUser(String user) {
        handlerProperties.setUser(user);
    }

    public void setPassword(String pass) {
        handlerProperties.setPassword(pass);
    }

    public void setPort(String port) {
        handlerProperties.setPort(port);
    }

    public void setDatabase(String database) {
        handlerProperties.setDatabase(database);
    }

    public void setFormat(String format) {
        handlerProperties.setFormat(format.toLowerCase());
    }

    public void setHost(String host) {
        handlerProperties.setHost(host);
    }

    public void setAuth(String auth) {
        handlerProperties.setAuth(auth);
    }

    public void setRootName(String rootName) {
        handlerProperties.setRootName(rootName);
    }

    public void setTransformName(String transformName) {
        handlerProperties.setTransformName(transformName);
    }

    public void setTransformParams(String transformParams) {
        handlerProperties.setTransformParams(transformParams);
    }

    public void setCollections(String collections) {
        handlerProperties.setCollections(collections);
    }

    public void setOrg(String org) {
        handlerProperties.setOrg(org);
    }

    public void setSchema(String schema) {
        handlerProperties.setSchema(schema);
    }

    public void setApplication(String application) {
        handlerProperties.setApplication(application);
    }

    public void setImageDb(String imageDb) {
        handlerProperties.setImageDb(imageDb);
    }

    public void setImageProperty(String imageProperty) {
        handlerProperties.setImageProperty(imageProperty);
    }

    public void setImageFormat(String imageFormat) {
        handlerProperties.setImageFormat(imageFormat);
    }

    public void setImageCollection(String imageCollection) {
        handlerProperties.setImageCollection(imageCollection);
    }

    public void setImageKeyProps(String imageKeyProps) {
        handlerProperties.setImageKeyProps(imageKeyProps);
    }

    public void setGateway(String gateway) {
        handlerProperties.setGateway(Boolean.parseBoolean(gateway));
    }

    public void setSsl(String ssl) {
        handlerProperties.setSsl(Boolean.parseBoolean(ssl));
    }

    public void setTruststore(String truststore) {
        handlerProperties.setTruststore(truststore);
    }

    public void setTruststoreFormat(String truststoreFormat) {
        handlerProperties.setTruststoreFormat(truststoreFormat);
    }

    public void setTruststorePassword(String truststorePassword) {
        handlerProperties.setTruststorePassword(truststorePassword);
    }

    public void setBatchSize(String batchSize) {
        handlerProperties.setBatchSize(Integer.parseInt(batchSize));
    }

    public void setThreadCount(String threadCount) {
        handlerProperties.setThreadCount(Integer.parseInt(threadCount));
    }

    public HandlerProperties getProperties() {
        return this.handlerProperties;
    }
}
