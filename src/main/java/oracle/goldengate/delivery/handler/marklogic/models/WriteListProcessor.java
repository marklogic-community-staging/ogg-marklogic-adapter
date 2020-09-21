package oracle.goldengate.delivery.handler.marklogic.models;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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
import oracle.goldengate.delivery.handler.marklogic.util.MarkLogicBatchFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * THIS CLASS IS NOT THREAD SAFE
 */
public class WriteListProcessor {
    final private static Logger logger = LoggerFactory.getLogger(WriteListProcessor.class);

    private final HandlerProperties handlerProperties;
    private final DataMovementManager manager;
    private final WriteBatcher writeBatcher;
    private final ObjectWriter objectWriter;
    private final JobTicket ticket;

    private Throwable error = null;

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

        Map<String, String> params = handlerProperties.getTransformParams();
        if(params != null) {
            params.keySet().forEach(paramName -> transform.addParameter(paramName, params.get(paramName)));
        }

        return transform;
    }

    private WriteBatcher newWriteBatcher(DataMovementManager manager, HandlerProperties handlerProperties) {
        WriteBatcher writeBatcher = manager.newWriteBatcher()
            .withBatchSize(handlerProperties.getBatchSize())
            .withThreadCount(handlerProperties.getThreadCount())
            .onBatchFailure((batch, failure) -> {
                this.error = failure;
            });

        if (handlerProperties.getTransformName() != null) {
            ServerTransform transform = newTransform(handlerProperties);
            writeBatcher = writeBatcher.withTransform(transform);
        }

        return writeBatcher;
    }

    private ObjectWriter newObjectWriter(HandlerProperties handlerProperties) {
        ObjectMapper mapper = "xml".equals(handlerProperties.getFormat()) ? new XmlMapper() : new ObjectMapper();
        mapper = mapper
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .setDateFormat(new StdDateFormat().withColonInTimeZone(true))
                .registerModule(new JavaTimeModule());

        ObjectWriter writer = mapper.writer().with(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN);
        writer = writer.withRootName("envelope");
        return writer;
    }

    public void process(Collection<WriteListItem> writeListItems) throws JsonProcessingException, MarkLogicBatchFailureException {

        Set<String> batchedUris = new HashSet<>();

        for (WriteListItem item : writeListItems) {
            AbstractWriteHandle handle;

            if (item.isBinary()) {
                handle = new BytesHandle().with(item.getBinary()).withFormat(Format.BINARY);
            } else {
                String docString = this.objectWriter.writeValueAsString(createEnvelope(item));
                handle = new StringHandle(docString).withFormat(Format.JSON);
            }

            DocumentMetadataHandle metadataHandle = new DocumentMetadataHandle();
            metadataHandle.getCollections().addAll(item.getCollection());
            metadataHandle.getCollections().addAll(this.handlerProperties.getCollections());

            String uri = item.getUri();
            if(batchedUris.contains(uri)) {
                // This is an exceptional case: 2 updates to the same row.
                // We want to make sure the previous row is written and committed first.
                logger.warn("Attempt to insert a duplicate row for table " + item.getSourceSchema() + "." + item.getSourceTable() + ": " + item.getUri());
                this.writeBatcher.flushAndWait();
                batchedUris.clear();
            }
            
            this.writeBatcher.add(uri, metadataHandle, handle);
            batchedUris.add(uri);
        }

        this.writeBatcher.flushAndWait();

        if(this.error != null) {
            throw new MarkLogicBatchFailureException("Error processing batch.", this.error);
        }
    }

    protected Map<String, Object> createEnvelope(WriteListItem item) {
        Map<String, Object> envelope = new HashMap<>();

        Map<String, Object> instance = new HashMap<>();
        Map<String, Object> schemaMap = new HashMap<>();
        instance.put(item.getSourceSchema(), schemaMap);
        schemaMap.put(item.getSourceTable(), item.getMap());

        envelope.put("instance", instance);
        envelope.put("headers", getHeaders(item));

        return envelope;
    }

    protected Map<String, Object> getHeaders(WriteListItem item) {
        Map<String, Object> headers = new HashMap<>();

        headers.put("importDate", OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        headers.put("sourceSchemaName", item.getSourceSchema());
        headers.put("sourceTableName", item.getSourceTable());
        headers.put("operation", item.getOperation());
        headers.put("uri", item.getUri());
        String previousUri = item.getOldUri();
        if (previousUri != null) {
            headers.put("previousUri", previousUri);
        }

        return headers;
    }
}
