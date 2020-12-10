package oracle.goldengate.delivery.handler.marklogic.listprocesor;

import com.marklogic.client.document.GenericDocumentManager;
import com.marklogic.client.query.DeleteQueryDefinition;
import com.marklogic.client.query.QueryManager;
import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;

import java.util.List;

public class TruncateListProcessor implements ListProcessor<String> {
    private final HandlerProperties handlerProperties;

    public TruncateListProcessor(HandlerProperties handlerProperties) {
        this.handlerProperties = handlerProperties;
    }

    public void process(List<String> items) {
        QueryManager queryMgr = handlerProperties.getClient().newQueryManager();
        DeleteQueryDefinition dm = queryMgr.newDeleteDefinition();

        if(items.size() > 0) {
            dm.setCollections(items.toArray(new String[items.size()]));
            queryMgr.delete(dm);
        }
    }
}
