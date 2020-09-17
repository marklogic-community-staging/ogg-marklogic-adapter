package org.goldengate.delivery.handler.marklogic;

import oracle.goldengate.datasource.*;
import oracle.goldengate.datasource.meta.*;
import oracle.goldengate.delivery.handler.marklogic.MarkLogicHandler;
import oracle.goldengate.util.DateString;
import oracle.goldengate.util.DsMetric;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

public class GGInputBuilder {
    protected MarkLogicHandler marklogicHandler;
    protected String schemaName;
    protected String tableName;
    protected final boolean isUpdate;
    protected GGDataSource.Status addStatus;
    protected GGDataSource.Status commitStatus;

    protected ArrayList<ColumnMetaData> columnMetaData = new ArrayList<>();
    protected List<DsColumn> columns = new ArrayList<>();
    protected boolean built = false;

    private GGInputBuilder(boolean isUpdate, MarkLogicHandler marklogicHandler) {
        this.isUpdate = isUpdate;
        this.marklogicHandler = marklogicHandler;
//        instantiateMarkLogicHandler();
    }

//    protected Properties loadProperties() {
//        Properties props = new Properties();
//
//        try (InputStream is = this.getClass().getResourceAsStream("/test.properties")) {
//            props.load(is);
//        } catch (Throwable t) {
//        }
//
//        try (InputStream is = this.getClass().getResourceAsStream("/test-local.properties")) {
//            props.load(is);
//        } catch (Throwable t) {
//        }
//
//        return props;
//    }
//
//    private void instantiateMarkLogicHandler() {
//        marklogicHandler = new MarkLogicHandler();
//
//        Properties props = loadProperties();
//
//        marklogicHandler.setHost(props.getProperty("gg.handler.marklogic.host"));
//        marklogicHandler.setDatabase(props.getProperty("gg.handler.marklogic.database"));
//        marklogicHandler.setPort(props.getProperty("gg.handler.marklogic.port"));
//        marklogicHandler.setSsl(props.getProperty("gg.handler.marklogic.ssl"));
//        marklogicHandler.setGateway(props.getProperty("gg.handler.marklogic.gateway"));
//        marklogicHandler.setUser(props.getProperty("gg.handler.marklogic.user"));
//        marklogicHandler.setPassword(props.getProperty("gg.handler.marklogic.password"));
//        marklogicHandler.setAuth(props.getProperty("gg.handler.marklogic.auth"));
//        marklogicHandler.setCollections(props.getProperty("gg.handler.marklogic.collections"));
//
//        marklogicHandler.setTruststore(props.getProperty("gg.handler.marklogic.truststore"));
//        marklogicHandler.setTruststoreFormat(props.getProperty("gg.handler.marklogic.truststoreFormat"));
//        marklogicHandler.setTruststorePassword(props.getProperty("gg.handler.marklogic.truststorePassword"));
//
//        marklogicHandler.setOrg(props.getProperty("gg.handler.marklogic.org"));
//        marklogicHandler.setSchema(props.getProperty("gg.handler.marklogic.schema"));
//        marklogicHandler.setApplication(props.getProperty("gg.handler.marklogic.application"));
//        marklogicHandler.setImageProperty(props.getProperty("gg.handler.marklogic.imageProperty"));
//        marklogicHandler.setImageFormat(props.getProperty("gg.handler.marklogic.imageFormat"));
//        marklogicHandler.setImageCollection(props.getProperty("gg.handler.marklogic.imageCollection"));
//        marklogicHandler.setImageDb(props.getProperty("gg.handler.marklogic.imageDb"));
//        marklogicHandler.setImageKeyProps(props.getProperty("gg.handler.marklogic.imageKeyProps"));
//
//        marklogicHandler.setBatchSize(props.getProperty("gg.handler.marklogic.batchSize"));
//        marklogicHandler.setThreadCount(props.getProperty("gg.handler.marklogic.threadCount"));
//
//        marklogicHandler.setOrg(props.getProperty("gg.handler.marklogic.org"));
//
//        marklogicHandler.setState(DataSourceListener.State.READY);
//    }

    public static GGInputBuilder newUpdate(MarkLogicHandler marklogicHandler) {
        return new GGInputBuilder(true, marklogicHandler);
    }

    public static GGInputBuilder newInsert(MarkLogicHandler marklogicHandler) {
        return new GGInputBuilder(false, marklogicHandler);
    }

    protected void verifyNotCommitted() throws IllegalStateException {
        if(this.built) {
            throw new IllegalStateException("GGInputBuilder has already been committed.");
        }
    }

    public GGInputBuilder withSchema(String schemaName) {
        verifyNotCommitted();
        this.schemaName = schemaName;
        return this;
    }

    public GGInputBuilder withTable(String tableName) {
        verifyNotCommitted();
        this.tableName = tableName;
        return this;
    }

    public GGInputBuilder withPrimaryKeyColumn(String columnName, String currentValue) {
        verifyNotCommitted();
        this.columns.add(new DsColumnComposite(new DsColumnAfterValue(currentValue)));
        this.columnMetaData.add(new ColumnMetaData(columnName, this.columnMetaData.size(), true));
        return this;
    }

    public GGInputBuilder withColumn(String columnName, String currentValue) {
        verifyNotCommitted();
        this.columns.add(new DsColumnComposite(new DsColumnAfterValue(currentValue)));
        ColumnMetaData columnMetaData = new ColumnMetaData(columnName, this.columnMetaData.size(), columnName.length(), (short)0, (short)DsType.GGType.GG_ASCII_V.getValue(), (short) DsType.GGSubType.GG_SUBTYPE_CHAR_TYPE.getValue(), (short)0, (short)0, (short)0, 0L, 0L, 0L, (short)0, (short)0);
        this.columnMetaData.add(columnMetaData);
        return this;
    }

    public GGInputBuilder withColumn(String columnName, DateString currentValue) {
        verifyNotCommitted();
        this.columns.add(new DsColumnComposite(new DsColumnAfterValue(currentValue)));
        ColumnMetaData columnMetaData = new ColumnMetaData(columnName, this.columnMetaData.size(), columnName.length(), (short)0, (short)DsType.GGType.GG_DATETIME.getValue(), (short) DsType.GGSubType.GGSubType_UNSET.getValue(), (short)0, (short)0, (short)0, 0L, 0L, 0L, (short)0, (short)0);
        this.columnMetaData.add(columnMetaData);
        return this;
    }

    public GGInputBuilder withColumn(String columnName, Instant currentValue) {
        return this.withColumn(columnName, new DateString(currentValue));
    }

    public GGInputBuilder withColumn(String columnName, ZonedDateTime currentValue) {
        return this.withColumn(columnName, new DateString(currentValue));
    }

    public GGInputBuilder commit() throws IllegalStateException {
        verifyNotCommitted();
        List<String> errors = new ArrayList<>();

        if(this.schemaName == null) {
            errors.add("schemaName is missing");
        }

        if(this.tableName == null) {
            errors.add("tableName is missing");
        }

        if(!errors.isEmpty()) {
            throw new IllegalStateException(String.join(", ", errors));
        }

        TableName ggTableName = new TableName(this.schemaName + "." + this.tableName);
        TableMetaData tableMetaData = new TableMetaData(ggTableName, this.columnMetaData);
        DsMetaData dsMetaData = new DsMetaData();
        dsMetaData.setTableMetaData(tableMetaData);

        long i = 233;
        long j = 32323;
        GGTranID ggTranID = GGTranID.getID(i, j);
        DsTransaction dsTransaction = new DsTransaction(ggTranID);
        DsEvent dsEvent = new DsEventManager.TxEvent(dsTransaction, ggTranID, dsMetaData, "Sample Transaction");

        DataSourceConfig ds = new DataSourceConfig();
        DsMetric dms = new DsMetric();

        marklogicHandler.setHandlerMetric(dms);
        marklogicHandler.init(ds, dsMetaData);

        DsRecord dsRecord = new DsRecord(columns);

        DsOperation.OpType opType = this.isUpdate ? DsOperation.OpType.DO_UPDATE : DsOperation.OpType.DO_INSERT;
        DsOperation dsOperation = new DsOperation(ggTableName, tableMetaData, opType, new DateString(ZonedDateTime.now()), 0l, 0l, dsRecord);

        this.addStatus = marklogicHandler.operationAdded(dsEvent, dsTransaction, dsOperation);
        this.commitStatus = marklogicHandler.transactionCommit(dsEvent, dsTransaction);

        this.built = true;

        return this;
    }


    public GGDataSource.Status getAddStatus() {
        return addStatus;
    }

    public GGDataSource.Status getCommitStatus() {
        return commitStatus;
    }

    public MarkLogicHandler getMarklogicHandler() {
        return marklogicHandler;
    }
}
