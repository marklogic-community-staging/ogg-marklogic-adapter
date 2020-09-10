package oracle.goldengate.delivery.handler.marklogic.models;

import com.marklogic.client.query.DeleteQueryDefinition;
import com.marklogic.client.query.QueryManager;
import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;

import java.util.ArrayList;

/**
 * Created by prawal on 1/23/17.
 */
public class TruncateList extends ArrayList<String> {
  public void commit(HandlerProperties handlerProperties) {
    QueryManager queryMgr = handlerProperties.getClient().newQueryManager();
    DeleteQueryDefinition dm = queryMgr.newDeleteDefinition();

    if(this.size() > 0) {
      dm.setCollections(this.toArray(new String[this.size()]));
      queryMgr.delete(dm);
    }
  }
}
