package oracle.goldengate.delivery.handler.marklogic.operations;

import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.meta.TableMetaData;
import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;
import oracle.goldengate.delivery.handler.marklogic.models.WriteListItem;

public class PkUpdateOperationHandler extends OperationHandler {

    public PkUpdateOperationHandler(HandlerProperties handlerProperties) {
        super(handlerProperties);
    }

    @Override
    public void process(TableMetaData tableMetaData, Op op) throws Exception {

        //Get previous key
        String olduri = prepareKey(tableMetaData, op, true, handlerProperties);

        // deletes are handle after writes as it is currently implemented
        //handlerProperties.deleteList.add(olduri);

        String baseUri = prepareKey(tableMetaData, op, false, handlerProperties);
        WriteListItem item = new WriteListItem(
                baseUri,
                getDataMap(baseUri, tableMetaData, op, false),
                WriteListItem.INSERT,
                tableMetaData.getTableName(),
                handlerProperties
        );

        // PK updates are not guaranteed to have all of the values for the columns
        // When using the data hub transform, it will use the old URI to find the
        // document to merge with, write the document with the new URI and then the
        // delete will remove the document with the old URI
        item.setOldUri(olduri);

        // Code was created to make sure that an update doesn't delete the record if the
        // same update is written multiple times.
        if (item.getUri().equalsIgnoreCase(olduri) == Boolean.FALSE) {
            // deletes are handle after writes as it is currently implemented
            handlerProperties.deleteList.add(olduri);
        }


        processOperation(item);

        handlerProperties.totalUpdates++;
    }

}
