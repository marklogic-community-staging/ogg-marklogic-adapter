import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.DeleteListener;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.query.StructuredQueryDefinition;
import oracle.goldengate.datasource.DataSourceListener;
import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;
import oracle.goldengate.delivery.handler.marklogic.MarkLogicHandler;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

public abstract class AbstractMarkLogicTest {
    protected MarkLogicHandler marklogicHandler;

    protected Properties loadProperties() {
        Properties props = new Properties();

        try(InputStream is = this.getClass().getResourceAsStream("/test.properties")) {
            props.load(is);
        } catch(IOException ex) {}

        try(InputStream is = this.getClass().getResourceAsStream("/test-local.properties")) {
            props.load(is);
        } catch(IOException ex) {}

        return props;
    }

    protected void setUp() throws IOException {
        marklogicHandler = new MarkLogicHandler();

        Properties props = loadProperties();

        marklogicHandler.setHost(props.getProperty("gg.handler.marklogic.host"));
        marklogicHandler.setDatabase(props.getProperty("gg.handler.marklogic.database"));
        marklogicHandler.setPort(props.getProperty("gg.handler.marklogic.port"));
        marklogicHandler.setSsl(props.getProperty("gg.handler.marklogic.ssl"));
        marklogicHandler.setGateway(props.getProperty("gg.handler.marklogic.gateway"));
        marklogicHandler.setUser(props.getProperty("gg.handler.marklogic.user"));
        marklogicHandler.setPassword(props.getProperty("gg.handler.marklogic.password"));
        marklogicHandler.setAuth(props.getProperty("gg.handler.marklogic.auth"));
        marklogicHandler.setCollections(props.getProperty("gg.handler.marklogic.collections"));

        marklogicHandler.setTruststore(props.getProperty("gg.handler.marklogic.truststore"));
        marklogicHandler.setTruststoreFormat(props.getProperty("gg.handler.marklogic.truststoreFormat"));
        marklogicHandler.setTruststorePassword(props.getProperty("gg.handler.marklogic.truststorePassword"));

        marklogicHandler.setOrg(props.getProperty("gg.handler.marklogic.org"));
        marklogicHandler.setSchema(props.getProperty("gg.handler.marklogic.schema"));
        marklogicHandler.setApplication(props.getProperty("gg.handler.marklogic.application"));
        marklogicHandler.setImageProperty(props.getProperty("gg.handler.marklogic.imageProperty"));
        marklogicHandler.setImageFormat(props.getProperty("gg.handler.marklogic.imageFormat"));
        marklogicHandler.setImageCollection(props.getProperty("gg.handler.marklogic.imageCollection"));
        marklogicHandler.setImageDb(props.getProperty("gg.handler.marklogic.imageDb"));
        marklogicHandler.setImageKeyProps(props.getProperty("gg.handler.marklogic.imageKeyProps"));

        marklogicHandler.setBatchSize(props.getProperty("gg.handler.marklogic.batchSize"));
        marklogicHandler.setThreadCount(props.getProperty("gg.handler.marklogic.threadCount"));

        marklogicHandler.setOrg(props.getProperty("gg.handler.marklogic.org"));

        marklogicHandler.setState(DataSourceListener.State.READY);
    }

    protected void tearDown() {
        deleteTestCollection(marklogicHandler.getProperties());
        marklogicHandler.destroy();
    }

    protected DocumentManager getDocumentManager(HandlerProperties props) {
        // defaults to json
        if ("xml".equals(props.getFormat())) {
            return props.getClient().newXMLDocumentManager();
        } else {
            return props.getClient().newJSONDocumentManager();
        }
    }

    protected ObjectMapper getObjectMapper(String format) {
        // defaults to json
        if ("xml".equals(format)) {
            return new XmlMapper();
        } else {
            return new ObjectMapper();
        }
    }

    protected HashMap<String, Object> readDocument(String uri, HandlerProperties props)
            throws IOException {
        // need an assert that checks the document in the DB
        DocumentManager mgr = getDocumentManager(props);

        InputStreamHandle handle = new InputStreamHandle();
        mgr.read(uri, handle);

        ObjectMapper mapper = getObjectMapper(props.getFormat());
        return mapper.readValue(handle.get(), HashMap.class);
    }

    protected void deleteTestCollection(HandlerProperties props) {
        DataMovementManager dmm = props.getClient().newDataMovementManager();
        QueryManager qm = props.getClient().newQueryManager();
        StructuredQueryBuilder sqb = qm.newStructuredQueryBuilder();

        String org = props.getOrg();
        String prefix = (org == null) ? "/" : "/" + org + "/";

        StructuredQueryDefinition query = sqb.collection(prefix + "ogg_test");

        QueryBatcher batcher = dmm.newQueryBatcher(query);
        batcher.withConsistentSnapshot()
                .onUrisReady(new DeleteListener());
        dmm.startJob(batcher);

        batcher.awaitCompletion();
        dmm.stopJob(batcher);
    }

}
