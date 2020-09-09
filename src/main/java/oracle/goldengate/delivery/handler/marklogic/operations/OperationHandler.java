package oracle.goldengate.delivery.handler.marklogic.operations;

import oracle.goldengate.datasource.DsColumn;
import oracle.goldengate.datasource.adapt.Col;
import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.meta.ColumnMetaData;
import oracle.goldengate.datasource.meta.ColumnName;
import oracle.goldengate.datasource.meta.DsType;
import oracle.goldengate.datasource.meta.TableMetaData;

import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;
import oracle.goldengate.delivery.handler.marklogic.models.WriteListItem;
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
    /**
     * @param tableMetaData
     *            - Table meta data
     * @param op
     *            - The current operation.
     * @param useBeforeValues
     *            - If true before values will be used, else after values will
     *            be used.
     * @return void
     */
    protected void processOperation(WriteListItem item) throws Exception {
        handlerProperties.writeList.add(item);
    }
    
    protected void addBinary(String imageUri, byte[] binary, TableMetaData tableMetaData, Col col) throws Exception {
    	WriteListItem item = new WriteListItem(
    		imageUri,
    		col.getBinary(),
            WriteListItem.INSERT,
            tableMetaData.getTableName(),
            handlerProperties.getImageCollection()
        );

        processOperation(item);
        handlerProperties.totalBinaryInserts++;
    }

    protected HashMap<String, Object> getDataMap(TableMetaData tableMetaData, Op op, boolean useBefore) throws Exception {

        HashMap<String, Object> dataMap = new HashMap<String, Object>();
        dataMap.put("headers", headers());

        for (Col col : op) {
            ColumnMetaData columnMetaData = tableMetaData.getColumnMetaData(col.getIndex());

           /* This was commented out in the original source
            if (useBefore) {
                if (col.getBefore() != null) {
                    dataMap.put(columnMetaData.getOriginalColumnName(), col.getBeforeValue());
                }
            } else {
                if (col.getAfter() != null) {
                    dataMap.put(columnMetaData.getOriginalColumnName(), col.getAfterValue());
                }
            }
            */

            // Use after values if present
            // TODO Create camelCase column names
            //If column is binary, then format image URI, add URI property to dataMap,
            //  then create new WriteListItem with binary content and add to WriteList.
            
            if(columnMetaData.getGGDataSubType() == DsType.GGSubType.GG_SUBTYPE_BINARY.getValue()) {
            	String imageUri = createImageUri(tableMetaData, op);
            	dataMap.put(handlerProperties.getImageProperty()+ "Uri", imageUri);
            	
            	//Insert binary document from Blob column
            	addBinary(imageUri, col.getBinary(), tableMetaData, col);
            } else {
                if (col.getAfter() != null) {
                    dataMap.put(columnMetaData.getOriginalColumnName(), col.getAfterValue());
                } else if (col.getBefore() != null) {
                    dataMap.put(columnMetaData.getOriginalColumnName(), col.getBeforeValue());
                }
            }
        }
        return dataMap;
    }

    protected String prepareKey(TableMetaData tableMetaData, Op op, boolean useBefore) throws NoSuchAlgorithmException {

        StringBuilder stringBuilder = new StringBuilder();

        String delimiter = "";


        for (ColumnMetaData columnMetaData : tableMetaData.getKeyColumns()  ) {

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

        return "/" + tableMetaData.getTableName().getShortName().toLowerCase() + "/"+ prepareKeyIndex(stringBuilder) + "." + handlerProperties.getFormat();
    }

    // Joining key column values and hashing
    private String prepareKeyIndex(StringBuilder sb) throws NoSuchAlgorithmException {

        MessageDigest md5 = MessageDigest.getInstance("MD5");
        String index;
        if(sb.length() > 0) {
            index = sb.toString();
            md5.update(StandardCharsets.UTF_8.encode(index));
            index =  String.format("%032x", new BigInteger(1, md5.digest()));
        } else {
            index = UUID.randomUUID().toString();
        }

        return index;
    }
    
    /*
     * Create the image key (filename), build from a configured list of properties of the table.
     */
    private void appendImageKey(TableMetaData tableMetaData, Op op, String keyProps, StringBuilder sb) {
    	String imageHashMapKey = handlerProperties.getSchema() + "." + tableMetaData.getTableName();
        HashMap<String, String[]> imageKeyProps = handlerProperties.getImageKeyProps();
        String[] imageProps = imageKeyProps.get(imageHashMapKey);
        
        if(imageProps == null || imageProps.length <=0) {
        	logger.warn("Image Key Properties missing for " + imageHashMapKey);
        }
        
        for (String imageProp : imageProps  ) {
        	ColumnMetaData columnMetaData = tableMetaData.getColumnMetaData(new ColumnName(imageProp));
            DsColumn col = op.getColumn(columnMetaData.getIndex());
            sb.append(handlerProperties.getUriDelimiter());
            sb.append(col.getAfterValue());
        }
    }
    
    /*
     * Create a URI to reference the image object that will be saved in another document
     *   and possibly in a different location, i.e. S3.
     */
    private String createImageUri(TableMetaData tableMetaData, Op op) {
    	StringBuilder stringBuilder = new StringBuilder();
    	stringBuilder.append(handlerProperties.getUriDelimiter());
    	stringBuilder.append(handlerProperties.getOrg());
    	stringBuilder.append(handlerProperties.getUriDelimiter());
    	stringBuilder.append(handlerProperties.getSchema());
    	stringBuilder.append(handlerProperties.getUriDelimiter());
    	stringBuilder.append(tableMetaData.getTableName());
    	appendImageKey(tableMetaData, op, handlerProperties.getImageProperty(), stringBuilder);
    	stringBuilder.append(".");
    	stringBuilder.append(handlerProperties.getImageFormat());
    	
    	return stringBuilder.toString();
    }

    private HashMap headers() {
        HashMap<String, Object> headers = new HashMap<String, Object>();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        Date date = new Date();
        String fdate = df.format(date);
        headers.put("importDate",fdate);
        return headers;
    };



}
