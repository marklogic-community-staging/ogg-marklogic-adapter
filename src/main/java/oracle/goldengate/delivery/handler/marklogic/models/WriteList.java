package oracle.goldengate.delivery.handler.marklogic.models;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.marklogic.client.document.BinaryDocumentManager;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.StringHandle;

import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;

/**
 * Created by prawal on 1/23/17.
 */
public class WriteList {

  private static String UPDATE_OPTION = "{operation:\"update\"}";
  private static String INSERT_OPTION = "{operation:\"insert\"}";

  private List<WriteListItem> items = new ArrayList<WriteListItem>();

  public void add(WriteListItem item) {
    this.items.add(item);
  }

  private DocumentManager getDocumentManager(HandlerProperties handlerProperties) {
    // defaults to json
    if ("xml".equals(handlerProperties.getFormat())) {
      return handlerProperties.getClient().newXMLDocumentManager();
    } else {
      return handlerProperties.getClient().newJSONDocumentManager();
    }
  }
  
  private BinaryDocumentManager getBinaryDocumentManager(HandlerProperties handlerProperties) {
	  BinaryDocumentManager docMgr = handlerProperties.getClient().newBinaryDocumentManager();
	  //docMgr.setMetadataExtraction(MetadataExtraction.PROPERTIES); //this may incur a performance hit
	  return docMgr;
  }

  private ObjectMapper getObjectMapper(HandlerProperties handlerProperties) {
    // defaults to json
    if ("xml".equals(handlerProperties.getFormat())) {
      return new XmlMapper();
    } else {
      return new ObjectMapper();
    }
  }

  public void commit(HandlerProperties handlerProperties) throws JsonProcessingException, IOException {

    if(this.items.size() > 0) {
      DocumentManager docMgr = getDocumentManager(handlerProperties);
      BinaryDocumentManager binaryDocMgr = getBinaryDocumentManager(handlerProperties);
      DocumentMetadataHandle metadataHandle = new DocumentMetadataHandle();
      DocumentMetadataHandle.DocumentCollections coll = metadataHandle.getCollections();

      for (WriteListItem item : items) {

          // assume updates vs inserts are handled in the transform
          if (handlerProperties.getTransformName() != null) {
            ServerTransform transform = getTransform(handlerProperties);
            if (item.getOperation() == WriteListItem.UPDATE) {
              transform.addParameter("options", UPDATE_OPTION);
            } else {
              transform.addParameter("options", INSERT_OPTION);
            }

            docMgr.setWriteTransform(transform);

            ObjectMapper mapper = getObjectMapper(handlerProperties);

            ObjectWriter writer = mapper.writer();
            /* if (handlerProperties.getRootName() != null) {
              writer = writer.withRootName(handlerProperties.getRootName());
            }*/  // Commented out the additional of the word root to the documents added to MarkLogic can't find a reason why we need it.
                 // It causes an issue when attempting to do a compare of the key value pair in UpdateNode I didn't remove the code
                 // Since we may find a business use for adding root to each document 

            HashMap<String, Object> node = item.getMap();
            StringHandle handle = new StringHandle(writer.writeValueAsString(node));

            coll.addAll(item.getCollection());
            coll.addAll(handlerProperties.getCollections());
            docMgr.write(item.getUri(), metadataHandle, handle);
            coll.clear();
          } else if (item.isBinary()) {
        	  //TODO insert images into a different database
        	  BytesHandle handle = new BytesHandle().with(item.getBinary());

              coll.addAll(item.getCollection());
              coll.addAll(handlerProperties.getCollections());
              binaryDocMgr.write(item.getUri(), metadataHandle, handle);
              coll.clear();
          } else {
            boolean docExists = false;

            if(item.getOperation() == WriteListItem.UPDATE && docMgr.exists(item.getUri()) != null) {
              docExists = true;
            }

            HashMap<String, Object> node = new HashMap<String,Object>(); 

            if(docExists == true) {
                node = updateNode(item, docMgr, handlerProperties);
            } else if(item.getOperation() == WriteListItem.UPDATE) {
              // skipping if update and doc doesn't exist
            } else {
              node = item.getMap();
            }

            if(docExists == false && item.getOperation() == WriteListItem.UPDATE) {
              // skipping if update and doc doesn't exist
            } else {


              ObjectMapper mapper = getObjectMapper(handlerProperties);

              ObjectWriter writer = mapper.writer();
              /* if (handlerProperties.getRootName() != null) {
                writer = writer.withRootName(handlerProperties.getRootName());
              } */
              // Commented out the additional of the word root to the documents added to MarkLogic can't find a reason why we need it.
              // It causes an issue when attempting to do a compare of the key value pair in UpdateNode.  I didn't remove the code
              // Since we may find a business use for adding root to each document

              StringHandle handle = new StringHandle(writer.writeValueAsString(node));

              coll.addAll(item.getCollection());
              coll.addAll(handlerProperties.getCollections());
              docMgr.write(item.getUri(), metadataHandle, handle);
              coll.clear();
            }
          }
      }
    }
  };

  private ServerTransform getTransform(HandlerProperties handlerProperties) {
    ServerTransform transform = new ServerTransform(handlerProperties.getTransformName());

    HashMap<String, String> params = handlerProperties.getTransformParams();
    for (String param : params.keySet()) {
      transform.addParameter(param, params.get(param));
    }

    return transform;
  }

  private HashMap<String, Object> updateNode(WriteListItem item, DocumentManager docMgr, HandlerProperties handlerProperties)
    throws IOException {

    InputStreamHandle handle = new InputStreamHandle();

    docMgr.read(item.getUri(), handle);

    ObjectMapper mapper = getObjectMapper(handlerProperties);
    HashMap<String, Object> original = mapper.readValue(handle.get(), HashMap.class);
    HashMap<String, Object> update = item.getMap();

    String key = null;
    Set<String> keys = original.keySet();
    Set<String> updateKeys = update.keySet();
    Iterator<String> itr = updateKeys.iterator();

    while (itr.hasNext()) {

      key = itr.next();
      if(keys.contains(key)) {
        original.put(key, update.get(key));
      }
    }

    return original;
  };

  public void clear() {
    items.clear();
  }

}
