package oracle.goldengate.delivery.handler.marklogic.util;

import oracle.goldengate.util.DateString;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;

public class DateStringUtil {

    public static DateTimeFormatter OFFSET_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
    public static DateTimeFormatter LOCAL_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    public static DateTimeFormatter INSTANT_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneId.of("Z"));;

    public static String toISO(DateString dateString) throws DateTimeParseException {
        try {
            ZonedDateTime zonedDateTime = dateString.getZonedDateTime();
            return OFFSET_FORMATTER.format(zonedDateTime);
        } catch(IllegalStateException ex) {}

        try {
            Instant instant = dateString.getInstant();
            return INSTANT_FORMATTER.format(instant);
        } catch(IllegalStateException ex) {}

        return toISO(dateString.toString());
    }

    public static final DateTimeFormatter SQL_PARSER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.nnnnnnnnn][nnnnnn][ ][XXX][X]");

    public static String toISO(String dateString) throws DateTimeParseException {
        TemporalAccessor dateTime = SQL_PARSER.parseBest(dateString, ZonedDateTime::from, LocalDateTime::from);

        return dateTime.isSupported(ChronoField.OFFSET_SECONDS) ?
            OFFSET_FORMATTER.format(dateTime) :
            LOCAL_FORMATTER.format(dateTime);
    }
}
