package oracle.goldengate.delivery.handler.marklogic.operations;

import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.meta.TableMetaData;
import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;
import oracle.goldengate.delivery.handler.marklogic.models.PendingItems;
import oracle.goldengate.delivery.handler.marklogic.models.WriteListItem;
import oracle.goldengate.delivery.handler.marklogic.models.WriteListItemFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class PkUpdateOperationHandler extends OperationHandler {

    public PkUpdateOperationHandler(HandlerProperties handlerProperties) {
        super(handlerProperties);
    }

    protected void processItems(final List<WriteListItem> items, List<WriteListItem> writeList, List<String> deleteList) {
        writeList.addAll(items);

        items.forEach(item -> {
            String oldUri = item.getOldUri();
            if (oldUri != null) {
                deleteList.add(oldUri);
            }
        });
    }

    @Override
    public void process(TableMetaData tableMetaData, Op op) throws Exception {
        PendingItems pendingItems = WriteListItemFactory.from(tableMetaData, op, false, WriteListItem.OperationType.PK_UPDATE, handlerProperties);

        processItems(Collections.unmodifiableList(pendingItems.getItems()), handlerProperties.writeList, handlerProperties.deleteList);
        processItems(Collections.unmodifiableList(pendingItems.getBinaryItems()), handlerProperties.binaryWriteList, handlerProperties.binaryDeleteList);

        handlerProperties.totalUpdates++;
    }

}
