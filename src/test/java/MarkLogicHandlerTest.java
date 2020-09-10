import oracle.goldengate.datasource.*;
import oracle.goldengate.datasource.meta.ColumnMetaData;
import oracle.goldengate.datasource.meta.DsMetaData;
import oracle.goldengate.datasource.meta.TableMetaData;
import oracle.goldengate.datasource.meta.TableName;
import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;
import oracle.goldengate.util.DateString;
import oracle.goldengate.util.DsMetric;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.testng.AssertJUnit;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by prawal on 1/18/17.
 */
public class MarkLogicHandlerTest extends AbstractMarkLogicTest {
    private DsEvent e;
    private DsTransaction dsTransaction;
    private TableName tableName;
    private DsMetaData dsMetaData;
    private TableMetaData tableMetaData;

    @BeforeMethod
    public void init() throws Exception {
        this.setUp();

        ArrayList<ColumnMetaData> columnMetaData = new ArrayList<>();

        columnMetaData.add(new ColumnMetaData("c1", 0));
        columnMetaData.add(new ColumnMetaData("c2", 1, true));
        columnMetaData.add(new ColumnMetaData("c3", 2));
        columnMetaData.add(new ColumnMetaData("c4", 3));
        columnMetaData.add(new ColumnMetaData("c5", 4));

        tableName = new TableName("ogg_test.new_table");

        tableMetaData = new TableMetaData(tableName, columnMetaData);

        dsMetaData = new DsMetaData();
        dsMetaData.setTableMetaData(tableMetaData);

        long i = 233;
        long j = 32323;

        GGTranID ggTranID = GGTranID.getID(i, j);

        dsTransaction = new DsTransaction(ggTranID);
        e = new DsEventManager.TxEvent(dsTransaction, ggTranID, dsMetaData, "Sample Transaction");

        DataSourceConfig ds = new DataSourceConfig();
        DsMetric dms = new DsMetric();

        marklogicHandler.setHandlerMetric(dms);
        marklogicHandler.init(ds, dsMetaData);

        // clear out the ogg-test collection
        deleteTestCollection(marklogicHandler.getProperties());
    }

    @AfterMethod
    public void clear() throws Exception {
        this.tearDown();
    }

    @Test
    public void testInsertJson() throws Exception {
        HandlerProperties props = marklogicHandler.getProperties();

        marklogicHandler.setFormat("json");

        DsColumn[] columns = new DsColumn[5];
        columns[0] = new DsColumnAfterValue("testing");
        columns[1] = new DsColumnAfterValue("2");
        columns[2] = new DsColumnAfterValue("3");
        columns[3] = new DsColumnAfterValue("2016-05-20 09:00:00");
        columns[4] = new DsColumnAfterValue("6");

        DsRecord dsRecord = new DsRecord(columns);

        DsOperation dsOperation = new DsOperation(tableName, tableMetaData, DsOperation.OpType.DO_INSERT, new DateString(ZonedDateTime.parse("2016-05-13T19:15:15.010Z")), 0l, 0l, dsRecord);
        GGDataSource.Status status = marklogicHandler.operationAdded(e, dsTransaction, dsOperation);
        marklogicHandler.transactionCommit(e, dsTransaction);
        AssertJUnit.assertEquals(GGDataSource.Status.OK, status);

        String expectedUri = "/my_org/ogg_test/new_table/c81e728d9d4c2f636f067f89cc14862c.json";
        HashMap<String, Object> updated = readDocument(expectedUri, props);
        Map<String, Object> envelope = (Map<String, Object>) updated.get("envelope");
        Map<String, Object> instance = (Map<String, Object>) envelope.get("instance");
        Map<String, Object> schema = (Map<String, Object>) instance.get("OGG_TEST");
        Map<String, Object> table = (Map<String, Object>) schema.get("NEW_TABLE");

        AssertJUnit.assertEquals("testing", table.get("c1"));
        AssertJUnit.assertEquals("2", table.get("c2"));
        AssertJUnit.assertEquals("3", table.get("c3"));
        AssertJUnit.assertEquals("2016-05-20 09:00:00", table.get("c4"));
        AssertJUnit.assertEquals("6", table.get("c5"));
    }

    @Test
    public void testInsertXml() throws Exception {
        HandlerProperties props = marklogicHandler.getProperties();
        props.setRootName("root");

        marklogicHandler.setFormat("xml");

        DsColumn[] columns = new DsColumn[5];
        columns[0] = new DsColumnAfterValue("testing");
        columns[1] = new DsColumnAfterValue("2");
        columns[2] = new DsColumnAfterValue("3");
        columns[3] = new DsColumnAfterValue("2016-05-20 09:00:00");
        columns[4] = new DsColumnAfterValue("6");

        DsRecord dsRecord = new DsRecord(columns);

        DsOperation dsOperation = new DsOperation(tableName, tableMetaData, DsOperation.OpType.DO_INSERT, new DateString(ZonedDateTime.parse("2016-05-13T19:15:15.010Z")), 0l, 0l, dsRecord);
        GGDataSource.Status status = marklogicHandler.operationAdded(e, dsTransaction, dsOperation);
        marklogicHandler.transactionCommit(e, dsTransaction);
        AssertJUnit.assertEquals(GGDataSource.Status.OK, status);

        String expectedUri = "/my_org/ogg_test/new_table/c81e728d9d4c2f636f067f89cc14862c.xml";

        HashMap<String, Object> envelope = readDocument(expectedUri, props);
        Map<String, Object> instance = (Map<String, Object>) envelope.get("instance");
        Map<String, Object> schema = (Map<String, Object>) instance.get("OGG_TEST");
        Map<String, Object> table = (Map<String, Object>) schema.get("NEW_TABLE");


        AssertJUnit.assertEquals("testing", table.get("c1"));
        AssertJUnit.assertEquals("2", table.get("c2"));
        AssertJUnit.assertEquals("3", table.get("c3"));
        AssertJUnit.assertEquals("2016-05-20 09:00:00", table.get("c4"));
        AssertJUnit.assertEquals("6", table.get("c5"));
    }

    @Test(enabled = false)
    public void testTransform() throws Exception {
        HandlerProperties props = marklogicHandler.getProperties();
        props.setRootName("root");

        marklogicHandler.setFormat("xml");

        // need to install a transform to test with
        props.setTransformName("run-flow");
        props.setTransformParams("entity=Policy,flow=policy,flowType=input");

        DsColumn[] columns = new DsColumn[5];
        columns[0] = new DsColumnAfterValue("testing transform");
        columns[1] = new DsColumnAfterValue("2");
        columns[2] = new DsColumnAfterValue("3");
        columns[3] = new DsColumnAfterValue("2016-05-20 09:00:00");
        columns[4] = new DsColumnAfterValue("6");

        DsRecord dsRecord = new DsRecord(columns);

        DsOperation dsOperation = new DsOperation(tableName, tableMetaData, DsOperation.OpType.DO_INSERT, new DateString(ZonedDateTime.parse("2016-05-13T19:15:15.010Z")), 0l, 0l, dsRecord);
        GGDataSource.Status status = marklogicHandler.operationAdded(e, dsTransaction, dsOperation);
        marklogicHandler.transactionCommit(e, dsTransaction);
        AssertJUnit.assertEquals(GGDataSource.Status.OK, status);

        String uri = "/new_table/c81e728d9d4c2f636f067f89cc14862c.xml";
        HashMap<String, Object> updated = readDocument(uri, props);

        AssertJUnit.assertEquals("testing transform", updated.get("c1"));
        AssertJUnit.assertEquals("2", updated.get("c2"));
        AssertJUnit.assertEquals("3", updated.get("c3"));
        AssertJUnit.assertEquals("2016-05-20 09:00:00", updated.get("c4"));
        AssertJUnit.assertEquals("6", updated.get("c5"));
    }

    @Test(enabled = false)
    public void testUpdateJson() throws Exception {
        testInsertJson();

        HandlerProperties props = marklogicHandler.getProperties();
        props.setRootName("root");

        marklogicHandler.setFormat("json");

        DsColumn[] columns = new DsColumn[5];
        columns[0] = new DsColumnComposite(new DsColumnAfterValue("puneet"), new DsColumnBeforeValue("testing"));
        columns[1] = new DsColumnAfterValue("2");
        columns[2] = new DsColumnComposite(new DsColumnAfterValue("600"), new DsColumnBeforeValue("3"));
        columns[3] = new DsColumnComposite(new DsColumnAfterValue("new date"), new DsColumnBeforeValue("some date"));
        columns[4] = new DsColumnComposite(new DsColumnAfterValue("600"), new DsColumnBeforeValue("6"));

        DsRecord dsRecord = new DsRecord(columns);

        DsOperation dsOperation = new DsOperation(tableName, tableMetaData, DsOperation.OpType.DO_UPDATE, new DateString(ZonedDateTime.parse("2016-05-13T19:15:15.010Z")), 0l, 0l, dsRecord);
        GGDataSource.Status status = marklogicHandler.operationAdded(e, dsTransaction, dsOperation);
        marklogicHandler.transactionCommit(e, dsTransaction);
        AssertJUnit.assertEquals(GGDataSource.Status.OK, status);

        String uri = "/new_table/c81e728d9d4c2f636f067f89cc14862c.json";
        HashMap<String, Object> updated = readDocument(uri, props);

        AssertJUnit.assertEquals("puneet", updated.get("c1"));
        AssertJUnit.assertEquals("2", updated.get("c2"));
        AssertJUnit.assertEquals("600", updated.get("c3"));
        AssertJUnit.assertEquals("new date", updated.get("c4"));
        AssertJUnit.assertEquals("600", updated.get("c5"));
    }

    @Test(enabled = false)
    public void testUpdateXml() throws Exception {
        testInsertXml();

        HandlerProperties props = marklogicHandler.getProperties();
        props.setRootName("root");

        marklogicHandler.setFormat("xml");

        DsColumn[] columns = new DsColumn[5];
        columns[0] = new DsColumnComposite(new DsColumnAfterValue("puneet"), new DsColumnBeforeValue("testing"));
        columns[1] = new DsColumnAfterValue("2");
        columns[2] = new DsColumnComposite(new DsColumnAfterValue("600"), new DsColumnBeforeValue("3"));
        columns[3] = new DsColumnComposite(new DsColumnAfterValue("new date 2"), new DsColumnBeforeValue("some date"));
        columns[4] = new DsColumnComposite(new DsColumnAfterValue("600"), new DsColumnBeforeValue("6"));

        DsRecord dsRecord = new DsRecord(columns);

        DsOperation dsOperation = new DsOperation(tableName, tableMetaData, DsOperation.OpType.DO_UPDATE, new DateString(ZonedDateTime.parse("2016-05-13T19:15:15.010Z")), 0l, 0l, dsRecord);
        GGDataSource.Status status = marklogicHandler.operationAdded(e, dsTransaction, dsOperation);
        marklogicHandler.transactionCommit(e, dsTransaction);
        AssertJUnit.assertEquals(GGDataSource.Status.OK, status);

        // assert that checks the document in the DB
        String uri = "/new_table/c81e728d9d4c2f636f067f89cc14862c.xml";
        HashMap<String, Object> updated = readDocument(uri, props);

        AssertJUnit.assertEquals("puneet", updated.get("c1"));
        AssertJUnit.assertEquals("2", updated.get("c2"));
        AssertJUnit.assertEquals("600", updated.get("c3"));
        AssertJUnit.assertEquals("new date 2", updated.get("c4"));
        AssertJUnit.assertEquals("600", updated.get("c5"));
    }

    @Test(enabled = false)
    public void testTruncate() {
        DsColumn[] columns = new DsColumn[5];
    /*
    columns[0] = new DsColumnAfterValue("testNormal");
    columns[1] = new DsColumnAfterValue("2");
    columns[2] = new DsColumnAfterValue("3");
    columns[3] = new DsColumnAfterValue("2016-05-20 09:00:00");
    columns[4] = new DsColumnAfterValue("6");
    */

        DsRecord dsRecord = new DsRecord(columns);
        DsOperation dsOperation = new DsOperation(tableName, tableMetaData, DsOperation.OpType.DO_TRUNCATE, new DateString(ZonedDateTime.parse("2016-05-13T19:15:15.010Z")), 0l, 0l, dsRecord);
        GGDataSource.Status status = marklogicHandler.operationAdded(e, dsTransaction, dsOperation);
        marklogicHandler.transactionCommit(e, dsTransaction);
        AssertJUnit.assertEquals(GGDataSource.Status.OK, status);
        marklogicHandler.destroy();
    }

    @Test(enabled = false)
    public void testAuth() {
        String status = "digest";
        HandlerProperties handle = new HandlerProperties();
        String auth = "digest";

        handle.setAuth(auth);

        AssertJUnit.assertEquals("digest", handle.getAuth());
    }
}
