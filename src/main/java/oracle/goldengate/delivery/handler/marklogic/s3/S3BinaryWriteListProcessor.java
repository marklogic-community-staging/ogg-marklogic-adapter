package oracle.goldengate.delivery.handler.marklogic.s3;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;
import oracle.goldengate.delivery.handler.marklogic.listprocesor.ListProcessor;
import oracle.goldengate.delivery.handler.marklogic.models.WriteListItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class S3BinaryWriteListProcessor implements ListProcessor<WriteListItem> {
    final private static Logger logger = LoggerFactory.getLogger(S3BinaryWriteListProcessor.class);
    private static final int MAX_RETRY_COUNT = 5;
    private static final int RETRY_DELAY = 15;

    private String stagingDirectory = "";

    private final HandlerProperties handlerProperties;

    private String bucketName;

    public S3BinaryWriteListProcessor(HandlerProperties handlerProperties) {
        this.handlerProperties = handlerProperties;
    }

    protected void processInternal(TransferManager transferManager, List<StagedItem> items, int retriesLeft) {
        if(items.size() > 0) {
            CountDownLatch latch = new CountDownLatch(items.size());
            Map<Upload, WriteListItem> uploads = new HashMap<>();
            List<WriteListItem> failedItemsBackingList = new LinkedList<>();
            List<WriteListItem> failedItems = Collections.synchronizedList(failedItemsBackingList);
            for (StagedItem stagedItem : items) {
                WriteListItem item = stagedItem.getWriteListItem();
                byte[] blob = item.getBinary();

                ObjectMetadata objectMetadata = new ObjectMetadata();
                objectMetadata.setContentLength(blob.length);

                try (InputStream input = new ByteArrayInputStream(blob)) {
                    PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, item.getUri(), input, objectMetadata)
                        .withGeneralProgressListener(progressEvent -> {
                            switch (progressEvent.getEventType()) {
                                case TRANSFER_COMPLETED_EVENT:
                                    latch.countDown();
                                    break;
                                case TRANSFER_FAILED_EVENT:
                                    failedItems.add(item);
                                    latch.countDown();
                                    break;
                                case TRANSFER_CANCELED_EVENT:
                                    latch.countDown();
                                    break;
                                default:
                                    break;
                            }
                        });

                    Upload upload = transferManager.upload(putObjectRequest);
                    uploads.put(upload, item);
                } catch (IOException ex) {
                    // should never be thrown, ByteArrayInputStream doesn't throw IOException
                }
            }

            try {
                latch.await();
            } catch (InterruptedException ex) { /* NOOP */ }

            if(failedItemsBackingList.size() > 0) {
                if(retriesLeft <= 0) {
                    logger.error("Errors uploading {} files to S3, but retry count exceeded.", failedItemsBackingList.size());
                } else {
                    logger.warn("Errors uploading {} files to S3, retrying in {} seconds.", failedItemsBackingList.size(), RETRY_DELAY);
                    try { TimeUnit.SECONDS.sleep(RETRY_DELAY); } catch (InterruptedException ex) { /* NOOP */ }
//                    processInternal(transferManager, failedItemsBackingList, retriesLeft - 1);
                }
            }
        }
    }

    protected List<StagedItem> stageFilesLocally(List<WriteListItem> items) throws IOException {
        Path stagePath = Paths.get(stagingDirectory);
        List<StagedItem> stagedItems = new LinkedList<>();

        for(WriteListItem item : items) {
            byte[] blob = item.getBinary();
            if(blob != null && blob.length > 0) {
                try (InputStream input = new ByteArrayInputStream(blob)) {
                    Path targetFile = Files.createTempFile(stagePath, "ogg-", ".bin");
                    Files.copy(input, targetFile, StandardCopyOption.REPLACE_EXISTING);
                    stagedItems.add(new StagedItem(targetFile, item));
                }
            }
        }

        return stagedItems;
    }

    public void process(List<WriteListItem> items) {
        try {
            List<StagedItem> stagedItems = Collections.unmodifiableList(stageFilesLocally(items));
            TransferManager transferManager = TransferManagerBuilder.standard().build();
            processInternal(transferManager, stagedItems, MAX_RETRY_COUNT);
        } catch(IOException ex) {

        }


    }
}
