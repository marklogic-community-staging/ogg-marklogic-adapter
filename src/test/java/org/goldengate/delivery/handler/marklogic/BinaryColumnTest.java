package org.goldengate.delivery.handler.marklogic;

import com.marklogic.client.document.BinaryDocumentManager;
import com.marklogic.client.document.DocumentDescriptor;
import oracle.goldengate.delivery.handler.marklogic.util.HashUtil;
import org.goldengate.delivery.handler.testing.AbstractGGTest;
import org.goldengate.delivery.handler.testing.GGInputBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

public class BinaryColumnTest extends AbstractGGTest {

    @Test
    public void testBinaryColumn() throws IOException {
        byte[] binary = this.readBinary("/30201.jpg");

        GGInputBuilder builder = GGInputBuilder.newInsert(this.markLogicHandler)
            .withSchema("ogg_test")
            .withTable("new_table")
            .withPrimaryKeyColumn("PK", "BlobTest")
            .withColumn("BLOB_DATA", binary)
            .commit();

        String expectedUri = "/my_org/ogg_test/new_table/" + HashUtil.hash("\"BlobTest\"") + ".json";
        String expectedImageUri = "/my_org/ogg_test/new_table/" + HashUtil.hash("\"BlobTest\"") + "/blobData.jpg";

        Map<String, Object> document = readDocument(expectedUri, builder.getMarklogicHandler().getProperties());
        Map<String, Object> instance = getInstance(document, "ogg_test", "new_table");

        Assert.assertEquals(instance.get("blobDataUri"), expectedImageUri);

        BinaryDocumentManager binaryDocMgr = this.getBinaryDatabaseClient().newBinaryDocumentManager();
        DocumentDescriptor bdd = binaryDocMgr.exists(expectedImageUri);
        Assert.assertNotNull(bdd);
    }

    @Test
    public void testNullBinaryColumn() throws IOException {
        byte[] binary = this.readBinary("/30201.jpg");

        GGInputBuilder builder = GGInputBuilder.newInsert(this.markLogicHandler)
            .withSchema("ogg_test")
            .withTable("new_table")
            .withPrimaryKeyColumn("PK", "NullBlobTest")
            .withColumn("BLOB_DATA", (byte[])null)
            .commit();

        String expectedUri = "/my_org/ogg_test/new_table/" + HashUtil.hash("\"NullBlobTest\"") + ".json";
        String expectedImageUri = "/my_org/ogg_test/new_table/" + HashUtil.hash("\"NullBlobTest\"") + "/BLOB_DATA.jpg";

        Map<String, Object> document = readDocument(expectedUri, builder.getMarklogicHandler().getProperties());
        Map<String, Object> instance = getInstance(document, "ogg_test", "new_table");

        Assert.assertEquals(instance.get("blobDataUri"), null);

        BinaryDocumentManager binaryDocMgr = this.getBinaryDatabaseClient().newBinaryDocumentManager();
        DocumentDescriptor bdd = binaryDocMgr.exists(expectedImageUri);
        Assert.assertNull(bdd);
    }
}
