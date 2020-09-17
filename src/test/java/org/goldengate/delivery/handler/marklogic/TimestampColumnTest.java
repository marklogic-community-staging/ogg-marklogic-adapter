package org.goldengate.delivery.handler.marklogic;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Map;

public class TimestampColumnTest extends AbstractGGTest {

    protected Map<String, Object> getInstance(Map<String, Object> document, String schemaName, String tableName) {
        Map<String, Object> envelope = (Map<String, Object>) document.get("envelope");
        Map<String, Object> instance = (Map<String, Object>) envelope.get("instance");
        Map<String, Object> schema = (Map<String, Object>) instance.get(schemaName.toUpperCase());
        Map<String, Object> table = (Map<String, Object>) schema.get(tableName.toUpperCase());
        return table;
    }

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
