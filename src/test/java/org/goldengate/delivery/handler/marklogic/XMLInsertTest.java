package org.goldengate.delivery.handler.marklogic;

import org.goldengate.delivery.handler.testing.AbstractGGTest;
import org.goldengate.delivery.handler.testing.GGInputBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

public class XMLInsertTest extends AbstractGGTest {

    @Test
    public void testXmlInsert() throws IOException {
        this.markLogicHandler.setFormat("xml");
        GGInputBuilder builder = GGInputBuilder.newInsert(this.markLogicHandler)
            .withSchema("ogg_test")
            .withTable("new_table")
            .withColumn("c1", "testing")
            .withPrimaryKeyColumn("c2", "XMLInsertTest")
            .withColumn("c3", "3")
            .withColumn("c4", "2016-05-20 09:00:00")
            .withColumn("c5", "6")
            .commit();

        String expectedUri = "/my_org/ogg_test/new_table/" + this.md5("XMLInsertTest") + ".xml";

        Map<String, Object> document = readDocument(expectedUri, builder.getMarklogicHandler().getProperties());
        Map<String, Object> instance = getInstance(document, "ogg_test", "new_table");

        Assert.assertEquals(instance.get("c1"), "testing");
        Assert.assertEquals(instance.get("c2"), "XMLInsertTest");
        Assert.assertEquals(instance.get("c3"), "3");
        Assert.assertEquals(instance.get("c4"), "2016-05-20 09:00:00");
        Assert.assertEquals(instance.get("c5"), "6");
    }
}
