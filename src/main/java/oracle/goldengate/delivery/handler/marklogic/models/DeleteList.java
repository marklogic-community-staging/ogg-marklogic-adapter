package oracle.goldengate.delivery.handler.marklogic.models;

import com.marklogic.client.document.GenericDocumentManager;
import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;

import java.util.ArrayList;

/**
 * Created by prawal on 1/23/17.
 */
public class DeleteList extends ArrayList<String> {
  public void commit(HandlerProperties handlerProperties) {
    GenericDocumentManager docMgr = handlerProperties.getClient().newDocumentManager();
    this.forEach(docMgr::delete);
  }
}
