package oracle.goldengate.delivery.handler.marklogic.util;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class HashUtil {
    public static String hash(Collection<String> values) {
        Object valuesArray[] = values.toArray(new String[values.size()]);
        return hash(valuesArray);
    }

    public static String hash(Object... values) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");

            List<String> encodedValues = new ArrayList<>();
            for (Object valueObj : values) {
                String idPart = valueObj == null ? "?null?" : URLEncoder.encode(valueObj.toString(), "UTF-8");
                encodedValues.add(idPart);
            }

            String encodedValue = String.join("/", encodedValues);
            messageDigest.update(StandardCharsets.UTF_8.encode(encodedValue));

            return String.format("%032x", new BigInteger(1, messageDigest.digest()));
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
    }
}
