package oracle.goldengate.delivery.handler.marklogic.operations;

import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.meta.TableMetaData;
import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;
import oracle.goldengate.delivery.handler.marklogic.models.WriteListItemFactory;

public class DeleteOperationHandler extends OperationHandler {


    public DeleteOperationHandler(HandlerProperties handlerProperties) {
        super(handlerProperties);
    }

    @Override
    public void process(TableMetaData tableMetaData, Op op) throws Exception {
        handlerProperties.deleteList.add(WriteListItemFactory.createUri(tableMetaData, op, true, handlerProperties));
        handlerProperties.totalDeletes++;
    }

}
