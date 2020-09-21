package oracle.goldengate.delivery.handler.marklogic.models;

import oracle.goldengate.datasource.DsColumn;
import oracle.goldengate.datasource.adapt.Col;
import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.meta.ColumnMetaData;
import oracle.goldengate.datasource.meta.DsType;
import oracle.goldengate.datasource.meta.TableMetaData;
import oracle.goldengate.datasource.meta.TableName;
import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;
import oracle.goldengate.delivery.handler.marklogic.util.DateStringUtil;
import oracle.goldengate.delivery.handler.marklogic.util.HashUtil;
import oracle.goldengate.delivery.handler.marklogic.util.JacksonUtil;
import oracle.goldengate.delivery.handler.marklogic.util.Pair;
import org.apache.commons.text.CaseUtils;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

public class WriteListItemFactory {
    protected static final char SQL_WORD_SEPARATORS[] = new char[]{'_'};
    public static PendingItems from(TableMetaData tableMetaData, Op op, boolean checkForKeyUpdate, WriteListItem.OperationType operationType, HandlerProperties handlerProperties) {
        PendingItems pendingItems = new PendingItems();

        final String baseUri = prepareKey(tableMetaData, op, false, handlerProperties);
        final String previousBaseUri = checkForKeyUpdate ?
            prepareKey(tableMetaData, op, true, handlerProperties) :
            null;

    protected static String sqlToCamelCase(String sqlName) {
        return CaseUtils.toCamelCase(sqlName, false, SQL_WORD_SEPARATORS);
    }

    protected static MarkLogicOp toMarkLogicOp(TableMetaData tableMetaData, Op op, HandlerProperties handlerProperties) {
        TableName tableName = tableMetaData.getTableName();
        MarkLogicOp markLogicOp = new MarkLogicOp()
            .withTable(tableName.getShortName())
            .withSchema(tableName.getSchemaName());

        tableMetaData.getKeyColumns()
            .stream()
            .map(ColumnMetaData::getColumnName)
            .map(WriteListItemFactory::sqlToCamelCase)
            .forEach(markLogicOp::withKeyColumn);

        op.forEach(col -> {
            ColumnMetaData columnMetaData = tableMetaData.getColumnMetaData(col.getIndex());
            String columnName ;
            String nullValue = handlerProperties.getNullValue();
            if ( handlerProperties.getRawName()) {
                    columnName = sqlToCamelCase(columnMetaData.getColumnName());
                }else{
                    columnName = columnMetaData.getColumnName();
                }

            if(columnMetaData.getDataType().getGGDataSubType() == DsType.GGSubType.GG_SUBTYPE_BINARY) {
                markLogicOp.withBinaryColumn(columnName);
            }
            if(col.hasBeforeValue()) {
                markLogicOp.withBeforeValue(columnName, columnValue(col.getBefore(), columnMetaData, nullValue));
            }
            if(col.hasAfterValue()) {
                markLogicOp.withAfterValue(columnName, columnValue(col.getAfter(), columnMetaData, nullValue));
            }
        });

        return markLogicOp;
    }

    public static Pair<Optional<String>, Optional<String>> getIds(MarkLogicOp markLogicOp) {
        Optional<String> id = markLogicOp.getAfterValues().map(values -> extractKey(markLogicOp.getKeyColumns(), values));
        Optional<String> previousId = markLogicOp.getBeforeValues().map(values -> extractKey(markLogicOp.getKeyColumns(), values));

        return Pair.of(previousId, id);
    }

    public static Pair<Optional<String>, Optional<String>> getUris(MarkLogicOp markLogicOp, HandlerProperties handlerProperties) {
        Pair<Optional<String>, Optional<String>> ids = getIds(markLogicOp);
        return getUris(markLogicOp, ids, handlerProperties);
    }

    public static String toUri(MarkLogicOp markLogicOp, String id, HandlerProperties handlerProperties, Optional<String> columnName) {
        List<String> uriParts = new LinkedList<>();

        uriParts.add("");
        Optional.ofNullable(handlerProperties.getUriPrefix()).ifPresent(uriParts::add);
        Optional.ofNullable(handlerProperties.getOrg()).ifPresent(uriParts::add);
        Optional.ofNullable(markLogicOp.getSchema()).map(String::toLowerCase).ifPresent(uriParts::add);
        Optional.ofNullable(markLogicOp.getTable()).map(String::toLowerCase).ifPresent(uriParts::add);
        uriParts.add(id);
        columnName.ifPresent(uriParts::add);

        return String.join("/", uriParts);
    }

    public static Pair<Optional<String>, Optional<String>> getUris(MarkLogicOp markLogicOp, Pair<Optional<String>, Optional<String>> ids, HandlerProperties handlerProperties) {
        return Pair.of(
            ids.getLeft().map(id -> toUri(markLogicOp, id, handlerProperties, Optional.empty())),
            ids.getRight().map(id -> toUri(markLogicOp, id, handlerProperties, Optional.empty()))
        );
    }

    public static Pair<Optional<String>, Optional<String>> getUris(MarkLogicOp markLogicOp, Pair<Optional<String>, Optional<String>> ids, HandlerProperties handlerProperties, String columnName) {
        return Pair.of(
            ids.getLeft().map(id -> toUri(markLogicOp, id, handlerProperties, Optional.ofNullable(columnName))),
            ids.getRight().map(id -> toUri(markLogicOp, id, handlerProperties, Optional.ofNullable(columnName)))
        );
    }

    public static PendingItems from(TableMetaData tableMetaData, Op op, boolean checkForKeyUpdate, WriteListItem.OperationType operationType, HandlerProperties handlerProperties) {

        MarkLogicOp markLogicOp = toMarkLogicOp(tableMetaData, op, handlerProperties);

        Pair<Optional<String>, Optional<String>> ids = getIds(markLogicOp);
        Pair<Optional<String>, Optional<String>> uris = getUris(markLogicOp, ids, handlerProperties);
        Optional<String> beforeUri = uris.getLeft();
        Optional<String> afterUri = uris.getRight();
        boolean uriChanged = (beforeUri.isPresent() && afterUri.isPresent() && (beforeUri.get() != afterUri.get()));

        boolean isKeyUpdate = false;
        if(checkForKeyUpdate) {
            Optional<String> previousId = ids.getLeft();
            Optional<String> id = ids.getRight();
            isKeyUpdate = ! (id.isPresent() && previousId.isPresent() && (id.get() == previousId.get()));
        }

        final PendingItems pendingItems = new PendingItems();
        final Collection<String> collections = makeCollections(markLogicOp.getSchema(), markLogicOp.getTable(), handlerProperties);
        Map<String, Object> doc = new HashMap<>();
        markLogicOp.getAfterValues().ifPresent(after -> {
            after.entrySet().forEach(afterEntry -> {
                String columnName = afterEntry.getKey();
                Object columnValue = afterEntry.getValue();
                if(markLogicOp.isBinary(columnName)) {
                    if (columnValue != null) {
                        Pair<Optional<String>, Optional<String>> binaryUris = getUris(markLogicOp, ids, handlerProperties, columnName);

                        byte blob[] = (byte[]) columnValue;

                        String binaryExtension = Optional.ofNullable(handlerProperties.getImageFormat()).map(fmt -> "." + fmt).orElse("");

                        WriteListItem binary = new WriteListItem();
                        if (uriChanged) {
                            binaryUris.getLeft().map(uri -> uri + binaryExtension).ifPresent(binary::setOldUri);
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
                        binaryUris.getRight().map(uri -> uri + binaryExtension).ifPresent(binary::setUri);

                        binary.setMap(null);
                        binary.setBinary(blob);
                        binary.setOperation(WriteListItem.INSERT);
                        binary.setCollection(makeBinaryCollections(collections, handlerProperties));
                        binary.setSourceSchema(markLogicOp.getSchema().toUpperCase());
                        binary.setSourceTable(markLogicOp.getTable().toUpperCase());
                        binary.setScn(op.getCsnStr());
                        binary.setTimestamp(op.getOperationTimestamp());
                        pendingItems.getBinaryItems().add(binary);
                        doc.put(columnName + "Uri", binary.getUri());
                    } else {
                        doc.put(columnName + "Uri", null);
                    }
                } else {
                    doc.put(columnName, columnValue);
                }
            });
        });

        WriteListItem item = new WriteListItem();

        afterUri.map(uri -> uri + "." + handlerProperties.getFormat()).ifPresent(item::setUri);
        if(uriChanged) {
            beforeUri.map(uri -> uri + "." + handlerProperties.getFormat()).ifPresent(item::setOldUri);
        }

        item.setMap(doc);
        item.setBinary(null);
        item.setOperation(operationType.getDescription());
        item.setCollection(collections);
        item.setSourceSchema(markLogicOp.getSchema().toUpperCase());
        item.setSourceTable(markLogicOp.getTable().toUpperCase());
        item.setScn(op.getCsnStr());
        item.setTimestamp(op.getOperationTimestamp());

        pendingItems.getItems().add(item);

        return pendingItems;
    }

    protected static Object columnValue(DsColumn column, ColumnMetaData columnMetaData, String nullValue) {
        if (column == null || column.isValueNull()) {
            return nullValue;
        }

        DsType columnDataType = columnMetaData.getDataType();
        DsType.GGType ggType = columnDataType.getGGDataType();
        DsType.GGSubType ggSubType = columnDataType.getGGDataSubType();

        if (ggSubType == DsType.GGSubType.GG_SUBTYPE_BINARY) {
            return column.hasBinaryValue() ? column.getBinary() : null;
        } else {
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
                        return DateStringUtil.toISO(column.getTimestamp());
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
                            return DateStringUtil.toISO(dateString);
                        } catch(DateTimeParseException ex) {
                            return dateString;
                        }
                            LocalDateTime localDateTime = LocalDateTime.parse(dateString, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                            return localDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                        } catch(DateTimeParseException ex) {}

                        return dateString;
                    }
                default:
                    return column.getValue();
            }
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

    protected static Collection<String> makeCollections(String schema, String table, HandlerProperties handlerProperties) {
        List<String> collections = new ArrayList<>();

        String org = handlerProperties.getOrg();
        String collectionPrefix = (org != null) ? "/" + org + "/" : "/";

        collections.add(collectionPrefix + schema.toLowerCase() + "/" + table.toLowerCase());
        collections.add(collectionPrefix + schema.toLowerCase());

        return collections;
    }

    protected static String extractKey(SortedSet<String> keyColumns, Map<String, Object> data) {
        List<String> keyData = extractKeyData(keyColumns, data);
        return HashUtil.hash(keyData);
    }

    protected static List<String> extractKeyData(SortedSet<String> keyColumns, Map<String, Object> data) {
        return keyColumns.stream()
            .map(data::get)
            .map(JacksonUtil::toJson)
            .collect(Collectors.toList());
    }
}
