package oracle.goldengate.delivery.handler.marklogic.listprocesor;

import com.marklogic.client.document.GenericDocumentManager;
import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;

import java.util.List;

public class DeleteListProcessor implements ListProcessor<String> {
    private final HandlerProperties handlerProperties;

    public DeleteListProcessor(HandlerProperties handlerProperties) {
        this.handlerProperties = handlerProperties;
    }

    public void process(List<String> items) {
        GenericDocumentManager docMgr = handlerProperties.getClient().newDocumentManager();
        items.forEach(docMgr::delete);
    }
}
