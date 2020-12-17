package oracle.goldengate.delivery.handler.marklogic.listprocesor;

import com.marklogic.client.io.DocumentMetadataHandle;

public class GGDocumentMetadataHandle extends DocumentMetadataHandle {

    protected int retryCount;

    public GGDocumentMetadataHandle() {
        super();

        this.retryCount = 0;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public void incrementRetryCount() {
        this.retryCount++;
    }
}
