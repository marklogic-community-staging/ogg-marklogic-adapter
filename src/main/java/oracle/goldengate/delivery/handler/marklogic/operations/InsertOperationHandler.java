package oracle.goldengate.delivery.handler.marklogic.operations;

import java.util.HashMap;

import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.meta.TableMetaData;
import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;
import oracle.goldengate.delivery.handler.marklogic.models.WriteListItem;
import oracle.goldengate.datasource.conf.DsHandler;


public class InsertOperationHandler extends OperationHandler {

    public InsertOperationHandler(HandlerProperties handlerProperties) {
        super(handlerProperties);
    }

    @Override
    public void process(TableMetaData tableMetaData, Op op) throws Exception {

        String baseUri = prepareKey(tableMetaData, op, false, handlerProperties);
        WriteListItem item = new WriteListItem(
                baseUri + "." + handlerProperties.getFormat(),
                getDataMap(baseUri, tableMetaData, op, false),
                WriteListItem.INSERT,
                tableMetaData.getTableName(),
                handlerProperties
        );

        processOperation(item);
        handlerProperties.totalInserts++;
    }

}
