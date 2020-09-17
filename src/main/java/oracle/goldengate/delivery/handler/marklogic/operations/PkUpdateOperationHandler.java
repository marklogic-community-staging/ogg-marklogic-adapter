package oracle.goldengate.delivery.handler.marklogic.operations;

import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.meta.TableMetaData;
import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;
import oracle.goldengate.delivery.handler.marklogic.models.WriteListItem;
import oracle.goldengate.delivery.handler.marklogic.models.WriteListItemFactory;

import java.util.Collection;

public class PkUpdateOperationHandler extends OperationHandler {

    public PkUpdateOperationHandler(HandlerProperties handlerProperties) {
        super(handlerProperties);
    }

    @Override
    public void process(TableMetaData tableMetaData, Op op) throws Exception {
        Collection<WriteListItem> items = WriteListItemFactory.from(tableMetaData, op, false, handlerProperties);
        handlerProperties.writeList.addAll(items);

        items.forEach(item -> {
            String oldUri = item.getOldUri();
            if (oldUri != null) {
                handlerProperties.deleteList.add(oldUri);
            }
        });

        handlerProperties.totalUpdates++;
    }

}
