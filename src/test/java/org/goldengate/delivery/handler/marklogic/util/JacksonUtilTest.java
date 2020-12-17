package org.goldengate.delivery.handler.marklogic.util;

import oracle.goldengate.delivery.handler.marklogic.util.JacksonUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class JacksonUtilTest {

    @Test
    public void testLocalDateTime() {
        LocalDateTime value = LocalDateTime.of(2020, 12, 15, 12, 30, 15, 1);
        String actual = JacksonUtil.toJson(value);
        String expected = "\"2020-12-15T12:30:15.000000001\"";

        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testZonedDateTime() {
        ZonedDateTime value = ZonedDateTime.of(2020, 12, 15, 12, 30, 15, 1, ZoneId.of("America/New_York"));
        String actual = JacksonUtil.toJson(value);
        String expected = "\"2020-12-15T12:30:15.000000001-05:00\"";

        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testString() {
        String value = "My \"String\"";
        String actual = JacksonUtil.toJson(value);
        String expected = "\"My \\\"String\\\"\"";

        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testNull() {
        String actual = JacksonUtil.toJson(null);
        String expected = "null";

        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testBigDecimal() {
        BigDecimal value = BigDecimal.valueOf(1234L);
        String actual = JacksonUtil.toJson(value);
        String expected = "1234";

        Assert.assertEquals(actual, expected);
    }
}
