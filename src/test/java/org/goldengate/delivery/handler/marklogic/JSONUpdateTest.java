package org.goldengate.delivery.handler.marklogic;

import oracle.goldengate.datasource.GGDataSource;
import oracle.goldengate.delivery.handler.marklogic.util.HashUtil;
import org.goldengate.delivery.handler.testing.AbstractGGTest;
import org.goldengate.delivery.handler.testing.GGInputBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

public class JSONUpdateTest extends AbstractGGTest {

    @Test
    public void testJsonUpdate() throws IOException {
        String expectedUri = "/my_org/ogg_test/new_table/" + HashUtil.hash("\"JSONUpdateTest\"") + ".json";

        GGInputBuilder builder = GGInputBuilder.newInsert(this.markLogicHandler)
            .withSchema("ogg_test")
            .withTable("new_table")
            .withPrimaryKeyColumn("VAL_PK", "JSONUpdateTest")
            .withColumn("VAL_ONE", "insertOne")
            .withColumn("VAL_TWO", "insertTwo")
            .withColumn("VAL_THREE", "insertThree")
            .commit();

        Assert.assertEquals(builder.getCommitStatus(), GGDataSource.Status.OK);

        Map<String, Object> document = readDocument(expectedUri, builder.getMarklogicHandler().getProperties());
        Map<String, Object> instance = getInstance(document, "ogg_test", "new_table");

        Assert.assertEquals(instance.get("valPk"), "JSONUpdateTest");
        Assert.assertEquals(instance.get("valOne"), "insertOne");
        Assert.assertEquals(instance.get("valTwo"), "insertTwo");
        Assert.assertEquals(instance.get("valThree"), "insertThree");

        GGInputBuilder updateBuilder = GGInputBuilder.newUpdate(this.markLogicHandler)
            .withSchema("ogg_test")
            .withTable("new_table")
            .withPrimaryKeyColumn("VAL_PK", "JSONUpdateTest")
            .withColumn("VAL_TWO", "updateTwo")
            .withColumn("VAL_THREE", (String)null)
            .commit();

        Assert.assertEquals(updateBuilder.getCommitStatus(), GGDataSource.Status.OK);

        Map<String, Object> updateDocument = readDocument(expectedUri, updateBuilder.getMarklogicHandler().getProperties());
        Map<String, Object> updateInstance = getInstance(updateDocument, "ogg_test", "new_table");

        Assert.assertEquals(updateInstance.get("valPk"), "JSONUpdateTest");
        Assert.assertEquals(updateInstance.get("valOne"), "insertOne");
        Assert.assertEquals(updateInstance.get("valTwo"), "updateTwo");
        Assert.assertNull(updateInstance.get("valThree"));
    }
}
