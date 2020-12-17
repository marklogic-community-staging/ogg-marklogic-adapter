package oracle.goldengate.delivery.handler.marklogic.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class JacksonUtil {

    public static final ObjectWriter JSON_WRITER = new JsonMapper()
        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        .setDateFormat(new StdDateFormat().withColonInTimeZone(true))
        .registerModule(new JavaTimeModule())
        .writer()
        .with(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
        ;

    public static String toJson(Object value) {
        String valueAsString;
        try {
            valueAsString = JacksonUtil.JSON_WRITER.writeValueAsString(value);
        } catch(JsonProcessingException ex) {
            valueAsString = value.toString();
        }

        return valueAsString;
    }
}
