package org.goldengate.delivery.handler.marklogic;

import org.goldengate.delivery.handler.testing.AbstractGGTest;
import org.goldengate.delivery.handler.testing.GGInputBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Map;

public class TimestampColumnTest extends AbstractGGTest {

    @Test
    public void testZonedDateTime() throws IOException {
        ZonedDateTime zonedDateTime = ZonedDateTime.parse("2020-09-17T12:29:36.072-04:00");

        GGInputBuilder builder = GGInputBuilder.newInsert(this.markLogicHandler)
            .withSchema("ogg_test")
            .withTable("new_table")
            .withPrimaryKeyColumn("PK_VALUE", "12345")
            .withColumn("TS_DTTM", zonedDateTime)
            .commit();

        String expectedUri = "/my_org/ogg_test/new_table/" + this.md5("12345") + ".json";
        Map<String, Object> document = readDocument(expectedUri, builder.getMarklogicHandler().getProperties());
        Map<String, Object> instance = getInstance(document, "ogg_test", "new_table");

        Assert.assertEquals(ZonedDateTime.parse((String) instance.get("tsDttm")).toInstant(), zonedDateTime.toInstant());
    }
}
