package org.goldengate.delivery.handler.marklogic;

import com.marklogic.client.document.BinaryDocumentManager;
import com.marklogic.client.document.DocumentDescriptor;
import org.goldengate.delivery.handler.testing.AbstractGGTest;
import org.goldengate.delivery.handler.testing.GGInputBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

public class NullColumnTest extends AbstractGGTest {

    @Test
    public void testNullStringColumn() throws IOException {
        GGInputBuilder builder = GGInputBuilder.newInsert(this.markLogicHandler)
            .withSchema("ogg_test")
            .withTable("new_table")
            .withPrimaryKeyColumn("PK", "NullStringTest")
            .withColumn("NULLABLE_TEXT", (String)null)
            .commit();

        String expectedUri = "/my_org/ogg_test/new_table/" + this.md5("NullStringTest") + ".json";

        Map<String, Object> document = readDocument(expectedUri, builder.getMarklogicHandler().getProperties());
        Map<String, Object> instance = getInstance(document, "ogg_test", "new_table");

        Assert.assertEquals(instance.get("nullableText"), null);
    }
}
