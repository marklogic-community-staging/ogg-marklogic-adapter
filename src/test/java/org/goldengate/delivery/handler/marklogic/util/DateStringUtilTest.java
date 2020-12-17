package org.goldengate.delivery.handler.marklogic.util;

import oracle.goldengate.delivery.handler.marklogic.util.DateStringUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DateStringUtilTest {
    @Test
    public void testLocal() {
        String actual = DateStringUtil.toISO("2012-03-27 11:13:22");
        String expected = "2012-03-27T11:13:22";

        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testLocalNanos() {
        String actual = DateStringUtil.toISO("2012-03-27 11:13:22.123456789");
        String expected = "2012-03-27T11:13:22.123456789";

        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testZoned() {
        String actual = DateStringUtil.toISO("2012-03-27 11:13:22 -05:00");
        String expected = "2012-03-27T11:13:22-05:00";

        Assert.assertEquals(actual, expected);
    }

    @Test
    public void testZonedNanos() {
        String actual = DateStringUtil.toISO("2012-03-27 11:13:22.123456789 -05:00");
        String expected = "2012-03-27T11:13:22.123456789-05:00";

        Assert.assertEquals(actual, expected);
    }
}
