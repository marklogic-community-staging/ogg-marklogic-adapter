package oracle.goldengate.delivery.handler.marklogic.operations;

import oracle.goldengate.datasource.DsColumn;
import oracle.goldengate.datasource.adapt.Col;
import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.meta.*;

import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;
import oracle.goldengate.delivery.handler.marklogic.models.WriteListItem;
import org.apache.commons.lang.WordUtils;
import org.apache.commons.text.CaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.security.*;

import static java.security.MessageDigest.*;


public abstract class OperationHandler {

    protected HandlerProperties handlerProperties = null;

    public OperationHandler(HandlerProperties handlerProperties) {
        this.handlerProperties = handlerProperties;
    }

    public abstract void process(TableMetaData tableMetaData, Op op) throws Exception;

    final private static Logger logger = LoggerFactory.getLogger(OperationHandler.class);

    protected void processOperation(WriteListItem item) throws Exception {
        handlerProperties.writeList.add(item);
    }

    protected void addBinary(String binaryUri, byte[] binary, TableMetaData tableMetaData, Col col) throws Exception {
        WriteListItem item = new WriteListItem(
                binaryUri,
                binary,
                WriteListItem.INSERT,
                tableMetaData.getTableName(),
                handlerProperties,
                handlerProperties.getImageCollection()
        );

        processOperation(item);
        handlerProperties.totalBinaryInserts++;
    }

    protected Map<String, Object> getDataMap(String baseUri, TableMetaData tableMetaData, Op op, boolean useBefore) throws Exception {

        HashMap<String, Object> dataMap = new HashMap<>();

        for (Col col : op) {
            ColumnMetaData columnMetaData = tableMetaData.getColumnMetaData(col.getIndex());

            // Use after values if present
            // If column is binary, then format image URI, add URI property to dataMap,
            // then create new WriteListItem with binary content and add to WriteList.

            if (columnMetaData.getGGDataSubType() == DsType.GGSubType.GG_SUBTYPE_BINARY.getValue()) {
                String binaryUri = createImageUri(baseUri, columnMetaData, op);
                String columnName = CaseUtils.toCamelCase(columnMetaData.getColumnName() + "_URI", false, new char[]{'_'});
                dataMap.put(columnName, binaryUri);

                //Insert binary document from Blob column
                addBinary(binaryUri, col.getBinary(), tableMetaData, col);
            } else {
                String columnName = CaseUtils.toCamelCase(columnMetaData.getColumnName(), false, new char[]{'_'});
                if (col.getAfter() != null) {
                    dataMap.put(columnName, col.getAfterValue());
                } else if (col.getBefore() != null) {
                    dataMap.put(columnName, col.getBeforeValue());
                }
            }
        }

        Map<String, Object> envelope = new HashMap<>();
        envelope.put("headers", headers(tableMetaData));

        Map<String, Object> instance = new HashMap<>();
        Map<String, Object> schema = new HashMap<>();

        TableName tableName = tableMetaData.getTableName();
        envelope.put("instance", instance);
        instance.put(tableName.getSchemaName().toUpperCase(), schema);
        schema.put(tableName.getShortName().toUpperCase(), dataMap);

        return envelope;
    }

    protected String prepareKey(TableMetaData tableMetaData, Op op, boolean useBefore, HandlerProperties handlerProperties) throws NoSuchAlgorithmException {
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
        return prefix + tableName.getSchemaName().toLowerCase() + "/" + tableName.getShortName().toLowerCase() + "/" + prepareKeyIndex(stringBuilder);
    }

    // Joining key column values and hashing
    private String prepareKeyIndex(StringBuilder sb) throws NoSuchAlgorithmException {

        MessageDigest md5 = MessageDigest.getInstance("MD5");
        String index;
        if (sb.length() > 0) {
            index = sb.toString();
            md5.update(StandardCharsets.UTF_8.encode(index));
            index = String.format("%032x", new BigInteger(1, md5.digest()));
        } else {
            index = UUID.randomUUID().toString();
        }

        return index;
    }

    /*
     * Create a URI to reference the image object that will be saved in another document
     *   and possibly in a different location, i.e. S3.
     */
    private String createImageUri(String baseUri, ColumnMetaData columnMetaData, Op op) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(baseUri);
        stringBuilder.append(handlerProperties.getUriDelimiter());
        stringBuilder.append(columnMetaData.getColumnName());
        stringBuilder.append(".");
        stringBuilder.append(handlerProperties.getImageFormat());

        return stringBuilder.toString();
    }

    private HashMap headers(TableMetaData tableMetaData) {
        HashMap<String, Object> headers = new HashMap<String, Object>();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        Date date = new Date();
        String fdate = df.format(date);
        headers.put("importDate", fdate);

        TableName tableName = tableMetaData.getTableName();
        headers.put("sourceSystemSchemaName", tableName.getSchemaName().toUpperCase());
        headers.put("sourceSystemTableName", tableName.getShortName().toUpperCase());

        return headers;
    }

    ;
}
