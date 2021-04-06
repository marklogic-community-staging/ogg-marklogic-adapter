package oracle.goldengate.delivery.handler.marklogic;

import com.marklogic.client.MarkLogicIOException;
import oracle.goldengate.datasource.*;
import oracle.goldengate.datasource.GGDataSource.Status;
import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.meta.DsMetaData;
import oracle.goldengate.datasource.meta.TableMetaData;
import oracle.goldengate.delivery.handler.marklogic.listprocesor.BinaryWriteListProcessor;
import oracle.goldengate.delivery.handler.marklogic.listprocesor.DeleteListProcessor;
import oracle.goldengate.delivery.handler.marklogic.listprocesor.TruncateListProcessor;
import oracle.goldengate.delivery.handler.marklogic.listprocesor.WriteListProcessor;
import oracle.goldengate.delivery.handler.marklogic.operations.OperationHandler;
import oracle.goldengate.delivery.handler.marklogic.util.DBOperationFactory;
import oracle.goldengate.util.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class MarkLogicHandler extends AbstractHandler {

    final private static Logger logger = LoggerFactory.getLogger(MarkLogicHandler.class);

    protected HandlerProperties handlerProperties;

    protected DBOperationFactory dbOperationFactory;

    protected MarkLogicHandler(HandlerProperties handlerProperties) {
        super();
        this.handlerProperties = handlerProperties;
        dbOperationFactory = new DBOperationFactory();
    }

    public MarkLogicHandler() {
        super();
        handlerProperties = new HandlerProperties();
        dbOperationFactory = new DBOperationFactory();
    }

    @Override
    public void init(DsConfiguration dsConfiguration, DsMetaData dsMetaData) {
        super.init(dsConfiguration, dsMetaData);

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
            } catch (Throwable throwable) {
                status = Status.ABEND;
                logger.error("Unable to process operation.", throwable);
            }
        } else {
            logger.error("Unable to instantiate operation handler for " + dsOperation.getOperationType().toString());
        }

        return status;
    }

    @Override
    public Status transactionBegin(DsEvent e, DsTransaction tx) {
        return super.transactionBegin(e, tx);
    }

    protected void commitWriteList() {
        boolean done = false;
        while(!done) {
            try {
                new WriteListProcessor(handlerProperties).process(handlerProperties.writeList);
                done = true;
            } catch(MarkLogicIOException ex) {
                logger.warn("There was an error communicating with the MarkLogic server. Waiting for 45 seconds to retry...");
                try {
                    TimeUnit.SECONDS.sleep(45);
                } catch (InterruptedException interruptedException) {
                    logger.warn("Sleep interrupted waiting to reconnect to MarkLogic server.", interruptedException);
                }
            }
        }
        handlerProperties.writeList.clear();
    }

    protected void commitBinaryWriteList() {
        boolean done = false;
        while(!done) {
            try {
                new BinaryWriteListProcessor(handlerProperties).process(handlerProperties.binaryWriteList);
                done = true;
            } catch(MarkLogicIOException ex) {
                logger.warn("There was an error communicating with the MarkLogic server. Waiting for 45 seconds to retry...");
                try {
                    TimeUnit.SECONDS.sleep(45);
                } catch (InterruptedException interruptedException) {
                    logger.warn("Sleep interrupted waiting to reconnect to MarkLogic server.", interruptedException);
                }
            }
        }
        handlerProperties.binaryWriteList.clear();
    }

    protected void commitDeleteList() {
        try {
            new DeleteListProcessor(this.handlerProperties).process(handlerProperties.deleteList);
            handlerProperties.deleteList.clear();
        } catch (Throwable t) {
            logger.error("Error flushing delete records ", t);
        }
    }

    protected void commitBinaryDeleteList() {
        try {
            new DeleteListProcessor(this.handlerProperties).process(handlerProperties.binaryDeleteList);
            handlerProperties.binaryDeleteList.clear();
        } catch (Throwable t) {
            logger.error("Error flushing binary delete records ", t);
        }
    }

    protected void commitTruncateList() {
        try {
            new TruncateListProcessor(this.handlerProperties).process(handlerProperties.truncateList);
            handlerProperties.truncateList.clear();
        } catch (Throwable t) {
            logger.error("Error flushing truncate records ", t);
        }
    }

    @Override
    public Status transactionCommit(DsEvent e, DsTransaction tx) {
        Status status = super.transactionCommit(e, tx);

        commitWriteList();
        commitBinaryWriteList();
        commitDeleteList();
        commitBinaryDeleteList();
        commitTruncateList();

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

    protected void initMarkLogicClient() throws Exception {
        this.handlerProperties.setClient(MarkLogicClientFactory.newClient(this.handlerProperties));
        this.handlerProperties.setBinaryClient(MarkLogicClientFactory.newBinaryClient(this.handlerProperties));
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

    public void setRawName(String rawName) {
        handlerProperties.setRawName(rawName);
    }

    public void setUriPrefix(String uriPrefix) {
        handlerProperties.setUriPrefix(uriPrefix);
    }

    public void setAddSchema(String addSchema) {
        handlerProperties.setAddSchema(addSchema);
    }
    //TODO: add extra functions for parameter for empty string return value
}
