import com.marklogic.client.document.BinaryDocumentManager;
import com.marklogic.client.document.DocumentDescriptor;
import oracle.goldengate.datasource.*;
import oracle.goldengate.datasource.meta.*;
import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;
import oracle.goldengate.util.DateString;
import oracle.goldengate.util.DsMetric;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.Assert;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MarkLogicBinaryHandlerTest extends AbstractMarkLogicTest {
    private DsEvent e;
    private DsTransaction dsTransaction;
    private TableName tableName;
    private TableMetaData tableMetaData;

    @BeforeMethod
    public void init() throws Exception {
        this.setUp();

        ArrayList<ColumnMetaData> columnMetaData = new ArrayList<>();

        columnMetaData.add(new ColumnMetaData("c1", 0));
        columnMetaData.add(new ColumnMetaData("c2", 1, true));
        columnMetaData.add(new ColumnMetaData("c3", 2));
        columnMetaData.add(new ColumnMetaData("c4", 3));

        ColumnMetaData binaryColMeta = new ColumnMetaData("BLOB_DATA",
            4,
            1024,
            (short) 0,
            (short) DsType.GGType.GGType_UNSET.getValue(), //DsType.GGType.GGType_UNSET,
            (short) DsType.GGSubType.GG_SUBTYPE_BINARY.getValue(), // DsType.GGSubType.GG_SUBTYPE_BINARY,
            (short) 0,
            (short) 0,
            (short) 0,
            0L,
            0L,
            0L,
            (short) 0,
            (short) 0);

        columnMetaData.add(binaryColMeta);

        tableName = new TableName("ogg_test.new_table");

        tableMetaData = new TableMetaData(tableName, columnMetaData);

        DsMetaData dsMetaData = new DsMetaData();
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
    }

    @AfterMethod
    public void clear() {
        this.tearDown();
    }

    @Test
    public void testInsertBinary() throws Exception {
        HandlerProperties props = marklogicHandler.getProperties();

        //Read image from resources
        //TODO move to separate method
        BufferedImage image = ImageIO.read(getClass().getResource("/30201.jpg"));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(image, "jpg", baos);
        baos.flush();
        byte[] binaryImage = baos.toByteArray();
        baos.close();

        marklogicHandler.setFormat("json");

        DsColumn[] columns = new DsColumn[5];
        columns[0] = new DsColumnAfterValue("testing");
        columns[1] = new DsColumnAfterValue("2");
        columns[2] = new DsColumnAfterValue("3");
        columns[3] = new DsColumnAfterValue("2016-05-20 09:00:00");
        columns[4] = new DsColumnAfterValue(null, binaryImage);

        DsRecord dsRecord = new DsRecord(columns);

        DsOperation dsOperation = new DsOperation(tableName, tableMetaData, DsOperation.OpType.DO_INSERT, new DateString(ZonedDateTime.parse("2016-05-13T19:15:15.010Z")), 0L, 0L, dsRecord);
        GGDataSource.Status status = marklogicHandler.operationAdded(e, dsTransaction, dsOperation);
        marklogicHandler.transactionCommit(e, dsTransaction);
        Assert.assertEquals(status, GGDataSource.Status.OK);

        String expectedUri = "/my_org/ogg_test/new_table/c81e728d9d4c2f636f067f89cc14862c.json";
        String expectedImageUri = "/my_org/ogg_test/new_table/c81e728d9d4c2f636f067f89cc14862c/BLOB_DATA.jpg";

        BinaryDocumentManager binaryDocMgr = props.getClient().newBinaryDocumentManager();
        DocumentDescriptor bdd = binaryDocMgr.exists(expectedImageUri);
        Assert.assertNotNull(bdd);

        HashMap<String, Object> updated = readDocument(expectedUri, props);
        Map<String, Object> envelope = (Map<String, Object>) updated.get("envelope");
        Map<String, Object> instance = (Map<String, Object>) envelope.get("instance");
        Map<String, Object> schema = (Map<String, Object>) instance.get("OGG_TEST");
        Map<String, Object> table = (Map<String, Object>) schema.get("NEW_TABLE");

        Assert.assertEquals(table.get("blobDataUri"), expectedImageUri);
    }
}
