package oracle.goldengate.delivery.handler.marklogic.operations;

import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.meta.TableMetaData;
import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;

public abstract class OperationHandler {

    protected HandlerProperties handlerProperties = null;

    public OperationHandler(HandlerProperties handlerProperties) {
        this.handlerProperties = handlerProperties;
    }

    public abstract void process(TableMetaData tableMetaData, Op op) throws Exception;
}
