package oracle.goldengate.delivery.handler.marklogic.models;

import oracle.goldengate.datasource.DsColumn;
import oracle.goldengate.datasource.adapt.Col;
import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.meta.ColumnMetaData;
import oracle.goldengate.datasource.meta.DsType;
import oracle.goldengate.datasource.meta.TableMetaData;
import oracle.goldengate.datasource.meta.TableName;
import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;
import org.apache.commons.text.CaseUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

public class WriteListItemFactory {
    public static PendingItems from(TableMetaData tableMetaData, Op op, boolean checkForKeyUpdate, WriteListItem.OperationType operationType, HandlerProperties handlerProperties) {
        PendingItems pendingItems = new PendingItems();

        final String baseUri = prepareKey(tableMetaData, op, false, handlerProperties);
        final String previousBaseUri = checkForKeyUpdate ?
            prepareKey(tableMetaData, op, true, handlerProperties) :
            null;

        TableName tableName = tableMetaData.getTableName();
        String schema = tableName.getSchemaName().toUpperCase();
        String table = tableName.getShortName().toUpperCase();

        Map<String, Object> columnValues = new HashMap<>();
        Collection<String> collections = makeCollections(tableName, handlerProperties);

        op.forEach(col -> {
            ColumnMetaData columnMetaData = tableMetaData.getColumnMetaData(col.getIndex());
            if (columnMetaData.getGGDataSubType() == DsType.GGSubType.GG_SUBTYPE_BINARY.getValue()) {
                String columnName = CaseUtils.toCamelCase(columnMetaData.getColumnName() + "_URI", false, new char[]{'_'});

                DsColumn bcol = col.getAfter();
                if (bcol == null) {
                    bcol = col.getBefore();
                }
                if (bcol != null && bcol.hasBinaryValue()) {
                    String binaryUri = createImageUri(baseUri, columnMetaData, handlerProperties);
                    //Insert binary document from Blob column
                    byte[] blob = bcol.getBinary();
                    WriteListItem binary = new WriteListItem();
                    binary.setUri(binaryUri);
                    if (previousBaseUri != null) {
                        String previousBinaryUri = createImageUri(previousBaseUri, columnMetaData, handlerProperties);
                        if (!previousBinaryUri.equals(binaryUri)) {
                            binary.setOldUri(previousBinaryUri);
                        }
                    }
                    binary.setMap(null);
                    binary.setBinary(blob);
                    binary.setOperation(WriteListItem.INSERT);
                    binary.setCollection(makeBinaryCollections(collections, handlerProperties));
                    binary.setSourceSchema(schema);
                    binary.setSourceTable(table);
                    pendingItems.getBinaryItems().add(binary);

                    // add the uri to the parent document
                    columnValues.put(columnName, binaryUri);
                } else {
                    // blank the uri in the parent document
                    columnValues.put(columnName, null);
                }
            } else {
                String columnName = CaseUtils.toCamelCase(columnMetaData.getColumnName(), false, new char[]{'_'});
                columnValues.put(columnName, getJsonValue(col, columnMetaData));
            }
        });

        WriteListItem item = new WriteListItem();
        item.setUri(baseUri + "." + handlerProperties.getFormat());
        if (previousBaseUri != null && !previousBaseUri.equals(baseUri)) {
            item.setOldUri(previousBaseUri + "." + handlerProperties.getFormat());
        }
        item.setMap(columnValues);
        item.setBinary(null);
        item.setOperation(operationType.getDescription());
        item.setCollection(collections);
        item.setSourceSchema(schema);
        item.setSourceTable(table);

        pendingItems.getItems().add(item);

        return pendingItems;
    }

    protected static Object getJsonValue(Col col, ColumnMetaData columnMetaData) {
        DsColumn column = (col.getAfter() != null) ? col.getAfter() : col.getBefore();
        if (column != null) {
            if (column.isValueNull()) {
                return null;
            }

            DsType columnDataType = columnMetaData.getDataType();
            DsType.GGType ggType = columnDataType.getGGDataType();
            DsType.GGSubType ggSubType = columnDataType.getGGDataSubType();

            switch (ggType) {
                case GG_16BIT_S:
                case GG_16BIT_U:
                case GG_32BIT_S:
                case GG_32BIT_U:
                case GG_64BIT_S:
                case GG_64BIT_U:
                case GG_REAL:
                case GG_IEEE_REAL:
                case GG_DOUBLE:
                case GG_IEEE_DOUBLE:
                case GG_DOUBLE_V:
                case GG_DOUBLE_F:
                case GG_DEC_U:
                case GG_DEC_LSS:
                case GG_DEC_LSE:
                case GG_DEC_TSS:
                case GG_DEC_TSE:
                case GG_DEC_PACKED:
                    return new BigDecimal(column.getValue());
                case GG_ASCII_V:
                case GG_ASCII_V_UP:
                case GG_ASCII_F:
                case GG_ASCII_F_UP:
                    switch (ggSubType) {
                        case GG_SUBTYPE_FLOAT:
                        case GG_SUBTYPE_FIXED_PREC:
                            return new BigDecimal(column.getValue());
                        default:
                            return column.getValue();
                    }
                case GG_DATETIME:
                case GG_DATETIME_V:
                    if (column.hasTimestampValue()) {
                        return column.getTimestamp().getInstant();
                    } else {
                        String dateString = column.getValue();

                        try {
                            ZonedDateTime zonedDateTime = ZonedDateTime.parse(dateString, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.nnnnnnnnn X"));
                            return zonedDateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                        } catch(DateTimeParseException ex) {}

                        try {
                            ZonedDateTime zonedDateTime = ZonedDateTime.parse(dateString, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss X"));
                            return zonedDateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                        } catch(DateTimeParseException ex) {}

                        try {
                            LocalDateTime localDateTime = LocalDateTime.parse(dateString, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.nnnnnnnnn"));
                            return localDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                        } catch(DateTimeParseException ex) {}

                        try {
                            LocalDateTime localDateTime = LocalDateTime.parse(dateString, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                            return localDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                        } catch(DateTimeParseException ex) {}

                        return dateString;
                    }
                default:
                    return column.getValue();
            }
        } else {
            return null;
        }
    }

    protected static Collection<String> makeBinaryCollections(Collection<String> baseCollections, HandlerProperties handlerProperties) {
        String binaryCollection = handlerProperties.getImageCollection();

        if (binaryCollection != null && binaryCollection.length() > 0) {
            List<String> binaryCollections = new ArrayList<>();
            binaryCollections.addAll(baseCollections);
            binaryCollections.add(binaryCollection);
            return binaryCollections;
        } else {
            return baseCollections;
        }
    }

    protected static Collection<String> makeCollections(TableName table, HandlerProperties handlerProperties) {
        List<String> collections = new ArrayList<>();

        String org = handlerProperties.getOrg();
        String collectionPrefix = (org != null) ? "/" + org + "/" : "/";

        collections.add(collectionPrefix + table.getSchemaName().toLowerCase() + "/" + table.getShortName().toLowerCase());
        collections.add(collectionPrefix + table.getSchemaName().toLowerCase());

        return collections;
    }

    public static String createUri(TableMetaData tableMetaData, Op op, boolean useBefore, HandlerProperties handlerProperties) {
        return prepareKey(tableMetaData, op, useBefore, handlerProperties) + "." + handlerProperties.getFormat();
    }

    protected static String prepareKey(TableMetaData tableMetaData, Op op, boolean useBefore, HandlerProperties handlerProperties) {
        StringBuilder stringBuilder = new StringBuilder();
        String delimiter = "";

        for (ColumnMetaData columnMetaData : tableMetaData.getKeyColumns()) {
            DsColumn column = op.getColumn(columnMetaData.getIndex());
            if (useBefore) {
                if (column.getBefore() != null) {
                    stringBuilder.append(delimiter);
                    stringBuilder.append(column.getBeforeValue());
                    delimiter = "_";
                }
            } else {
                if (column.getAfter() != null) {
                    stringBuilder.append(delimiter);
                    stringBuilder.append(column.getAfterValue());
                    delimiter = "_";
                }
            }
        }

        String org = handlerProperties.getOrg();
        String prefix = (org == null) ? "/" : "/" + org + "/";

        TableName tableName = tableMetaData.getTableName();
        return prefix + tableName.getSchemaName().toLowerCase() + "/" + tableName.getShortName().toLowerCase() + "/" + prepareKeyIndex(stringBuilder.toString());
    }

    // Hash the value of the index
    protected static String prepareKeyIndex(String value) {
        if (value != null && value.length() > 0) {
            try {
                MessageDigest messageDigest = MessageDigest.getInstance("MD5");
                messageDigest.update(StandardCharsets.UTF_8.encode(value));
                return String.format("%032x", new BigInteger(1, messageDigest.digest()));
            } catch (NoSuchAlgorithmException ex) {
                return value;
            }
        } else {
            return UUID.randomUUID().toString();
        }
    }

    /*
     * Create a URI to reference the image object that will be saved in another document
     *   and possibly in a different location, i.e. S3.
     */
    protected static String createImageUri(String baseUri, ColumnMetaData columnMetaData, HandlerProperties handlerProperties) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(baseUri);
        stringBuilder.append(handlerProperties.getUriDelimiter());
        stringBuilder.append(columnMetaData.getColumnName());
        stringBuilder.append(".");
        stringBuilder.append(handlerProperties.getImageFormat());

        return stringBuilder.toString();
    }
}
