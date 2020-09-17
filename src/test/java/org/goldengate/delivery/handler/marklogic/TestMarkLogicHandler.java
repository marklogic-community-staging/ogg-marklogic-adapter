package org.goldengate.delivery.handler.marklogic;

import com.marklogic.client.DatabaseClient;
import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;
import oracle.goldengate.delivery.handler.marklogic.MarkLogicHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMarkLogicHandler extends MarkLogicHandler {
    final private static Logger logger = LoggerFactory.getLogger(TestMarkLogicHandler.class);

    public TestMarkLogicHandler(HandlerProperties handlerProperties) {
        super(handlerProperties);
    }

    @Override
    protected void initMarkLogicClient() throws Exception {
        logger.info("Skipping MarkLogic client initialization");
    }
}
