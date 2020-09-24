package org.goldengate.delivery.handler.marklogic;

import org.goldengate.delivery.handler.testing.AbstractGGTest;
import org.goldengate.delivery.handler.testing.GGInputBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.TimeZone;

public class TimestampColumnTest extends AbstractGGTest {

    @Test
    public void testZonedDateTime() throws IOException {
        ZonedDateTime zonedDateTime = ZonedDateTime.parse("2020-09-17T12:29:36.072-04:00");

        GGInputBuilder builder = GGInputBuilder.newInsert(this.markLogicHandler)
            .withSchema("ogg_test")
            .withTable("new_table")
            .withPrimaryKeyColumn("PK_VALUE", "TimestampColumnTest")
            .withColumn("TS_DTTM", zonedDateTime)
            .commit();

        String expectedUri = "/my_org/ogg_test/new_table/" + this.md5("TimestampColumnTest") + ".json";
        Map<String, Object> document = readDocument(expectedUri, builder.getMarklogicHandler().getProperties());
        Map<String, Object> instance = getInstance(document, "ogg_test", "new_table");

        Assert.assertEquals(ZonedDateTime.parse((String) instance.get("tsDttm")).toInstant(), zonedDateTime.toInstant());
    }

    private static DateTimeFormatter SQL_LOCAL_DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.nnnnnnnnn");
    private static DateTimeFormatter SQL_LOCAL_SHORT_DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static DateTimeFormatter SQL_ZONED_DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.nnnnnnnnn X");
    private static DateTimeFormatter SQL_ZONED_SHORT_DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss X");

    @Test
    public void testLocalStringDateTime() throws IOException {
        ZoneId zoneId = TimeZone.getTimeZone("America/New_York").toZoneId();
        ZonedDateTime when = ZonedDateTime.parse("2020-09-23T17:58:10.994093000Z").withZoneSameInstant(zoneId);
        String sqlDateString = when.format(SQL_LOCAL_DATETIME_FORMATTER);

        GGInputBuilder builder = GGInputBuilder.newInsert(this.markLogicHandler)
            .withSchema("ogg_test")
            .withTable("new_table")
            .withPrimaryKeyColumn("PK_VALUE", "TimestampColumnTest")
            .withTimestampColumn("TS_DTTM", sqlDateString)
            .commit();

        String expectedUri = "/my_org/ogg_test/new_table/" + this.md5("TimestampColumnTest") + ".json";
        Map<String, Object> document = readDocument(expectedUri, builder.getMarklogicHandler().getProperties());
        Map<String, Object> instance = getInstance(document, "ogg_test", "new_table");

        Assert.assertEquals(ZonedDateTime.parse((String) instance.get("tsDttm")).toInstant(), when.toInstant());
    }

    @Test
    public void testLocalShortStringDateTime() throws IOException {
        ZoneId zoneId = TimeZone.getTimeZone("America/New_York").toZoneId();
        ZonedDateTime when = ZonedDateTime.parse("2020-09-23T17:58:10Z").withZoneSameInstant(zoneId);
        String sqlDateString = when.format(SQL_LOCAL_SHORT_DATETIME_FORMATTER);

        GGInputBuilder builder = GGInputBuilder.newInsert(this.markLogicHandler)
            .withSchema("ogg_test")
            .withTable("new_table")
            .withPrimaryKeyColumn("PK_VALUE", "TimestampColumnTest")
            .withTimestampColumn("TS_DTTM", sqlDateString)
            .commit();

        String expectedUri = "/my_org/ogg_test/new_table/" + this.md5("TimestampColumnTest") + ".json";
        Map<String, Object> document = readDocument(expectedUri, builder.getMarklogicHandler().getProperties());
        Map<String, Object> instance = getInstance(document, "ogg_test", "new_table");

        Assert.assertEquals(ZonedDateTime.parse((String) instance.get("tsDttm")).toInstant(), when.toInstant());
    }

    @Test
    public void testZonedStringDateTime() throws IOException {
        ZonedDateTime when = ZonedDateTime.parse("2020-09-23T17:58:10.994093000Z");
        String sqlDateString = when.format(SQL_ZONED_DATETIME_FORMATTER);

        GGInputBuilder builder = GGInputBuilder.newInsert(this.markLogicHandler)
            .withSchema("ogg_test")
            .withTable("new_table")
            .withPrimaryKeyColumn("PK_VALUE", "TimestampColumnTest")
            .withTimestampColumn("TS_DTTM", sqlDateString)
            .commit();

        String expectedUri = "/my_org/ogg_test/new_table/" + this.md5("TimestampColumnTest") + ".json";
        Map<String, Object> document = readDocument(expectedUri, builder.getMarklogicHandler().getProperties());
        Map<String, Object> instance = getInstance(document, "ogg_test", "new_table");

        Assert.assertEquals(ZonedDateTime.parse((String) instance.get("tsDttm")).toInstant(), when.toInstant());
    }

    @Test
    public void testZonedShortStringDateTime() throws IOException {
        ZonedDateTime when = ZonedDateTime.parse("2020-09-23T17:58:10Z");
        String sqlDateString = when.format(SQL_ZONED_SHORT_DATETIME_FORMATTER);

        GGInputBuilder builder = GGInputBuilder.newInsert(this.markLogicHandler)
            .withSchema("ogg_test")
            .withTable("new_table")
            .withPrimaryKeyColumn("PK_VALUE", "TimestampColumnTest")
            .withTimestampColumn("TS_DTTM", sqlDateString)
            .commit();

        String expectedUri = "/my_org/ogg_test/new_table/" + this.md5("TimestampColumnTest") + ".json";
        Map<String, Object> document = readDocument(expectedUri, builder.getMarklogicHandler().getProperties());
        Map<String, Object> instance = getInstance(document, "ogg_test", "new_table");

        Assert.assertEquals(ZonedDateTime.parse((String) instance.get("tsDttm")).toInstant(), when.toInstant());
    }

}
