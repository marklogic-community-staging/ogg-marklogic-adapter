package oracle.goldengate.delivery.handler.marklogic.models;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.JobTicket;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class WriteListProcessor {
    private final HandlerProperties handlerProperties;
    private final DataMovementManager manager;
    private final WriteBatcher writeBatcher;
    private final ObjectWriter objectWriter;
    private final JobTicket ticket;

    public WriteListProcessor(HandlerProperties handlerProperties) {
        this.handlerProperties = handlerProperties;
        this.manager = this.newDataMovementManager(handlerProperties);
        this.writeBatcher = this.newWriteBatcher(manager, handlerProperties);
        this.objectWriter = this.newObjectWriter(handlerProperties);
        this.ticket = this.manager.startJob(this.writeBatcher);
    }

    private DataMovementManager newDataMovementManager(HandlerProperties handlerProperties) {
        DatabaseClient client = handlerProperties.getClient();
        return client.newDataMovementManager();
    }

    private ServerTransform newTransform(HandlerProperties handlerProperties) {
        ServerTransform transform = new ServerTransform(handlerProperties.getTransformName());

        HashMap<String, String> params = handlerProperties.getTransformParams();
        for (String param : params.keySet()) {
            transform.addParameter(param, params.get(param));
        }

        return transform;
    }

    private WriteBatcher newWriteBatcher(DataMovementManager manager, HandlerProperties handlerProperties) {
        WriteBatcher writeBatcher = manager.newWriteBatcher()
                .withBatchSize(handlerProperties.getBatchSize())
                .withThreadCount(handlerProperties.getThreadCount());

        if(handlerProperties.getTransformName() != null) {
            ServerTransform transform = newTransform(handlerProperties);
            writeBatcher = writeBatcher.withTransform(transform);
        }

        return writeBatcher;
    }

    private ObjectWriter newObjectWriter(HandlerProperties handlerProperties) {
        ObjectMapper mapper = "xml".equals(handlerProperties.getFormat()) ?  new XmlMapper() : new ObjectMapper();
        ObjectWriter writer = mapper.writer();
        writer = writer.withRootName("envelope");
        return writer;
    }

    public void process(Collection<WriteListItem> writeListItems) throws JsonProcessingException {

        for(WriteListItem item : writeListItems) {
            AbstractWriteHandle handle;

            if (item.isBinary()) {
                handle = new BytesHandle().with(item.getBinary()).withFormat(Format.BINARY);
            } else {
                Map<String, Object> node = item.getMap();
                String docString = this.objectWriter.writeValueAsString(node);
                handle = new StringHandle(docString).withFormat(Format.JSON);
            }

            DocumentMetadataHandle metadataHandle = new DocumentMetadataHandle();
            metadataHandle.getCollections().addAll(item.getCollection());
            metadataHandle.getCollections().addAll(this.handlerProperties.getCollections());

            this.writeBatcher.add(item.getUri(), metadataHandle, handle);
        }

        this.writeBatcher.flushAndWait();
    }
}
