package oracle.goldengate.delivery.handler.marklogic.listprocesor;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;

public class BinaryWriteListProcessor extends WriteListProcessor {

    public BinaryWriteListProcessor(HandlerProperties handlerProperties) {
        super(handlerProperties);
    }

    @Override
    protected DataMovementManager newDataMovementManager(HandlerProperties handlerProperties) {
        DatabaseClient client = handlerProperties.getBinaryClient();
        return client.newDataMovementManager();
    }
}
