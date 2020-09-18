package oracle.goldengate.delivery.handler.marklogic.operations;

import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.meta.TableMetaData;
import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;
import oracle.goldengate.delivery.handler.marklogic.models.WriteListItem;
import oracle.goldengate.delivery.handler.marklogic.models.WriteListItemFactory;

import java.util.Collection;

public class InsertOperationHandler extends OperationHandler {
    public InsertOperationHandler(HandlerProperties handlerProperties) {
        super(handlerProperties);
    }

    @Override
    public void process(TableMetaData tableMetaData, Op op) {
        Collection<WriteListItem> items = WriteListItemFactory.from(tableMetaData, op, false, WriteListItem.OperationType.INSERT, handlerProperties);
        handlerProperties.writeList.addAll(items);
        handlerProperties.totalInserts++;
    }
}
