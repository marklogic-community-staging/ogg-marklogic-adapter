package org.goldengate.delivery.handler.marklogic;

import oracle.goldengate.datasource.GGDataSource;
import oracle.goldengate.delivery.handler.marklogic.util.HashUtil;
import oracle.goldengate.util.DateString;
import org.goldengate.delivery.handler.testing.AbstractGGTest;
import org.goldengate.delivery.handler.testing.GGInputBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;

public class JSONInsertTest extends AbstractGGTest {

    @Test
    public void testJsonInsert() throws IOException {
        GGInputBuilder builder = GGInputBuilder.newInsert(this.markLogicHandler)
            .withSchema("ogg_test")
            .withTable("new_table")
            .withScn(100L)
            .withColumn("c1", "testing")
            .withPrimaryKeyColumn("PK_A", "JSONInsertTest")
            .withPrimaryKeyColumn("PK_C", 12345L)
            .withPrimaryKeyColumn("PK_B", (String)null)
            .withColumn("c3", "3")
            .withTimestampColumn("LOCAL_DATETIME_STR", "2016-05-20 09:00:00")
            .withTimestampColumn("ZONED_DATETIME_STR", "2016-05-20 09:00:00 -04:00")
            .withColumn("DATESTRING_STRING_WITH_ZONE", new DateString("2016-05-20 09:45:00 -04:00"))
            .withColumn("DATESTRING_STRING", new DateString("2016-05-20 09:45:00"))
            .withColumn("DATESTRING_ZONED_DATETIME", new DateString(ZonedDateTime.of(2016, 5, 20, 9, 30, 0, 0, ZoneId.of("America/New_York"))))
            .withColumn("DATESTRING_INSTANT", new DateString(ZonedDateTime.of(2016, 5, 20, 9, 30, 0, 0, ZoneId.of("America/New_York")).toInstant()))
            .withColumn("c5", "6")
            .withColumn("NUMBER", 49)
            .commit();

        Assert.assertEquals(builder.getCommitStatus(), GGDataSource.Status.OK);

        String expectedUri = "/my_org/ogg_test/new_table/" + HashUtil.hash("\"JSONInsertTest\"", "null", "12345") + ".json";

        Map<String, Object> document = readDocument(expectedUri, builder.getMarklogicHandler().getProperties());
        Map<String, Object> instance = getInstance(document, "ogg_test", "new_table");

        Assert.assertEquals(instance.get("c1"), "testing");
        Assert.assertEquals(instance.get("pkA"), "JSONInsertTest");
        Assert.assertNull(instance.get("pkB"));
        Assert.assertEquals(instance.get("pkC"), 12345);
        Assert.assertEquals(instance.get("c3"), "3");
        Assert.assertEquals(instance.get("localDatetimeStr"), "2016-05-20T09:00:00");
        Assert.assertEquals(instance.get("zonedDatetimeStr"), "2016-05-20T09:00:00-04:00");
        Assert.assertEquals(instance.get("datestringStringWithZone"), "2016-05-20T09:45:00-04:00");
        Assert.assertEquals(instance.get("datestringString"), "2016-05-20T09:45:00");
        Assert.assertEquals(instance.get("datestringZonedDatetime"), "2016-05-20T09:30:00-04:00");
        Assert.assertEquals(instance.get("datestringInstant"), "2016-05-20T13:30:00Z");
        Assert.assertEquals(instance.get("c5"), "6");
        Assert.assertEquals(instance.get("number"), 49);
    }
}
