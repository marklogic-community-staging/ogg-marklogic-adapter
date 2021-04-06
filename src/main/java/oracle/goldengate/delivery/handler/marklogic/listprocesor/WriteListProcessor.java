package oracle.goldengate.delivery.handler.marklogic.listprocesor;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
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
import com.marklogic.client.io.marker.DocumentMetadataWriteHandle;
import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;
import oracle.goldengate.delivery.handler.marklogic.models.WriteListItem;
import oracle.goldengate.delivery.handler.marklogic.util.DateStringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * THIS CLASS IS NOT THREAD SAFE
 */
public class WriteListProcessor implements ListProcessor<WriteListItem> {
    final private static Logger logger = LoggerFactory.getLogger(WriteListProcessor.class);
    private static final int MAX_RETRY_COUNT = 50;

    private final HandlerProperties handlerProperties;
    private final DataMovementManager manager;
    private final WriteBatcher writeBatcher;
    private final ObjectWriter objectWriter;
    private final JobTicket ticket;

    // We need to be able to read from this map in multiple threads, so cannot use a HashMap.
    // ConcurrentHashMap retrieval operations do not lock in all cases.
//    private final ConcurrentHashMap<String, WriteListItemHolder> batchedItems = new ConcurrentHashMap<>();
    private List<WriteListItemHolder> retryList = new LinkedList<>();

    public WriteListProcessor(HandlerProperties handlerProperties) {
        this.handlerProperties = handlerProperties;
        this.manager = this.newDataMovementManager(handlerProperties);
        this.writeBatcher = this.newWriteBatcher(manager, handlerProperties);
        this.objectWriter = this.newObjectWriter(handlerProperties);
        this.ticket = this.manager.startJob(this.writeBatcher);
    }

    protected DataMovementManager newDataMovementManager(HandlerProperties handlerProperties) {
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
                Arrays.stream(batch.getItems()).forEach(writeEvent -> {
                    String uri = writeEvent.getTargetUri();
                    DocumentMetadataWriteHandle metadataWriteHandle = writeEvent.getMetadata();
                    if(metadataWriteHandle instanceof GGDocumentMetadataHandle) {
                        GGDocumentMetadataHandle ggDocumentMetadataHandle = (GGDocumentMetadataHandle) metadataWriteHandle;
                        if(ggDocumentMetadataHandle.getRetryCount() < MAX_RETRY_COUNT) {
                            ggDocumentMetadataHandle.incrementRetryCount();
                            synchronized(this.retryList) {
                                retryList.add(new WriteListItemHolder(uri, ggDocumentMetadataHandle, writeEvent.getContent()));
                            }
                        } else {
                            logger.error("Maximum retries ({}) exceeded for {}, discarding.", MAX_RETRY_COUNT, uri);
                        }
                    }
                });
            });

        if (handlerProperties.getTransformName() != null) {
            ServerTransform transform = newTransform(handlerProperties);
            writeBatcher = writeBatcher.withTransform(transform);
        }

        return writeBatcher;
    }

    private ObjectWriter newObjectWriter(HandlerProperties handlerProperties) {
        ObjectMapper mapper = "xml".equals(handlerProperties.getFormat()) ? new XmlMapper() : new JsonMapper();
        mapper = mapper
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .setDateFormat(new StdDateFormat().withColonInTimeZone(true))
                .registerModule(new JavaTimeModule());

        ObjectWriter writer = mapper.writer().with(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN);
        writer = writer.withRootName("envelope");
        return writer;
    }

    protected List<WriteListItemHolder> toHolderList(List<WriteListItem> writeListItems) {
        if(writeListItems == null) {
            return Collections.emptyList();
        } else {
            List<WriteListItemHolder> itemsToProcess = new LinkedList<>();
            for (WriteListItem item : writeListItems) {
                try {
                    WriteListItemHolder holder = new WriteListItemHolder(item);
                    itemsToProcess.add(holder);
                } catch(JsonProcessingException ex) {
                    logger.error("There was an error converting " + item.getUri() + ", discarding.", ex);
                }
            }
            return itemsToProcess;
        }

    }

    protected List<List<WriteListItemHolder>> toBatches(List<WriteListItemHolder> itemsToProcess) {
        if(itemsToProcess.isEmpty() || itemsToProcess.isEmpty()) {
            return Collections.emptyList();
        } else {
            List<List<WriteListItemHolder>> batches = new LinkedList<>();
            List<WriteListItemHolder> currentBatch = new LinkedList<>();
            batches.add(currentBatch);

            final Set<String> currentBatchUris = new HashSet<>();
            for (WriteListItemHolder holder : itemsToProcess) {
                String uri = holder.getUri();
                if (currentBatchUris.contains(uri)) {
                    currentBatch = new LinkedList<>();
                    batches.add(currentBatch);
                    currentBatchUris.clear();
                }
                currentBatchUris.add(uri);
                currentBatch.add(holder);
            }

            return batches;
        }
    }

    protected void processBatch(List<WriteListItemHolder> batch) {
        for (WriteListItemHolder holder : batch) {
            this.writeBatcher.add(holder.getUri(), holder.getMetadataHandle(), holder.getWriteHandle());
        }
        this.flushAndWait();

        synchronized(this.retryList) {
            while (!retryList.isEmpty()) {
                List<WriteListItemHolder> itemsToRetry = Collections.unmodifiableList(this.retryList);
                this.retryList = new LinkedList<>();
                for (WriteListItemHolder holder : itemsToRetry) {
                    this.writeBatcher.add(holder.getUri(), holder.getMetadataHandle(), holder.getWriteHandle());
                }
                this.flushAndWait();
            }
        }
    }

    public void process(List<WriteListItem> writeListItems) {
        toBatches(toHolderList(writeListItems)).forEach(this::processBatch);
    }

    protected Map<String, Object> createEnvelope(WriteListItem item) {
        Map<String, Object> envelope = new HashMap<>();
        Map<String, Object> instance = new HashMap<>();
        Map<String, Object> schemaMap = new HashMap<>();

        if (handlerProperties.getAddSchema().equals("true")){

            instance.put(item.getSourceSchema(), schemaMap);
            schemaMap.put(item.getSourceTable(), item.getMap());

        } else {

            instance.put(item.getSourceTable(), item.getMap());
            return instance;
        }
        //TODO: new condition to remove or include envelope
        envelope.put("instance", instance);
        envelope.put("headers", getHeaders(item));
        return envelope;
    }

    protected Map<String, Object> getHeaders(WriteListItem item) {
        Map<String, Object> headers = new HashMap<>();

        headers.put("importDate", OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        headers.put("schema", item.getSourceSchema());
        headers.put("table", item.getSourceTable());
        //TODO: add logic for flow or transform, the bellow code should only be use on transformation process
        //headers.put("operation", item.getOperation());
        headers.put("operationTimestamp", DateStringUtil.toISO(item.getTimestamp()));
        Optional.ofNullable(item.getScn()).map(Long::parseLong).ifPresent(scn -> headers.put("scn", scn));
        String previousUri = item.getOldUri();
        if (previousUri != null) {
            headers.put("previousUri", previousUri);
        }

        return headers;
    }

    protected void flushAndWait() {
        this.writeBatcher.flushAndWait();
    }

    class WriteListItemHolder {
        private final String uri;
        private final GGDocumentMetadataHandle metadataHandle;
        private final AbstractWriteHandle writeHandle;

        public WriteListItemHolder(String uri, GGDocumentMetadataHandle metadataHandle, AbstractWriteHandle writeHandle) {
            this.uri = uri;
            this.metadataHandle = metadataHandle;
            this.writeHandle = writeHandle;
        }

        public WriteListItemHolder(WriteListItem item) throws JsonProcessingException {
            AbstractWriteHandle handle;

            if (item.isBinary()) {
                handle = new BytesHandle().with(item.getBinary()).withFormat(Format.BINARY);
            } else {
                String docString = WriteListProcessor.this.objectWriter.writeValueAsString(createEnvelope(item));
                handle = new StringHandle(docString).withFormat(Format.JSON);
            }

            GGDocumentMetadataHandle metadataHandle = new GGDocumentMetadataHandle();
            metadataHandle.getCollections().addAll(item.getCollection());
            metadataHandle.getCollections().addAll(WriteListProcessor.this.handlerProperties.getCollections());

            this.metadataHandle = metadataHandle;
            this.writeHandle = handle;
            this.uri =  item.getUri();
        }

        public String getUri() {
            return uri;
        }

        public GGDocumentMetadataHandle getMetadataHandle() {
            return metadataHandle;
        }

        public AbstractWriteHandle getWriteHandle() {
            return writeHandle;
        }
    }
}
