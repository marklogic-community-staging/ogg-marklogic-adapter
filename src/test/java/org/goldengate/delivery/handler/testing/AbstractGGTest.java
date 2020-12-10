package org.goldengate.delivery.handler.testing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.DeleteListener;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.query.StructuredQueryDefinition;
import oracle.goldengate.delivery.handler.marklogic.HandlerProperties;
import oracle.goldengate.delivery.handler.marklogic.MarkLogicClientFactory;
import oracle.goldengate.delivery.handler.marklogic.MarkLogicHandler;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AbstractGGTest {
    protected HandlerProperties handlerProperties;
    protected DatabaseClient databaseClient;
    protected DatabaseClient binaryDatabaseClient;
    protected MarkLogicHandler markLogicHandler;

    @BeforeClass
    public void abstractGGTestBeforeClass() throws Exception {
        this.handlerProperties = newHandlerProperties();
        this.databaseClient = MarkLogicClientFactory.newClient(this.handlerProperties);
        this.binaryDatabaseClient = MarkLogicClientFactory.newBinaryClient(this.handlerProperties);
        this.handlerProperties.setClient(this.databaseClient);
        this.handlerProperties.setBinaryClient(this.binaryDatabaseClient);
        this.markLogicHandler = new TestMarkLogicHandler(this.handlerProperties);
    }

    @AfterMethod
    public void abstractGGTestAfterMethod() {
        deleteTestCollections(this.databaseClient);
        deleteTestCollections(this.binaryDatabaseClient);
    }

    @AfterClass
    public void abstractGGTestAfterClass() {
        this.markLogicHandler.destroy();
    }

    protected String md5(String value) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            messageDigest.update(StandardCharsets.UTF_8.encode(value));
            return String.format("%032x", new BigInteger(1, messageDigest.digest()));
        } catch (NoSuchAlgorithmException ex) {
            return value;
        }
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
        ObjectMapper mapper = "xml".equals(format) ? new XmlMapper() : new ObjectMapper();
        return mapper
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
            .setDateFormat(new StdDateFormat().withColonInTimeZone(true))
            .registerModule(new JavaTimeModule());
    }

    protected HashMap<String, Object> readDocument(String uri, HandlerProperties props) throws IOException {
        // need an assert that checks the document in the DB
        DocumentManager mgr = getDocumentManager(props);

        InputStreamHandle handle = new InputStreamHandle();
        mgr.read(uri, handle);

        ObjectMapper mapper = getObjectMapper(props.getFormat());
        return mapper.readValue(handle.get(), HashMap.class);
    }

    protected void deleteTestCollections(DatabaseClient databaseClient) {
        DataMovementManager dmm = databaseClient.newDataMovementManager();
        QueryManager qm = databaseClient.newQueryManager();
        StructuredQueryBuilder sqb = qm.newStructuredQueryBuilder();

        Collection<String> collections = this.handlerProperties.getCollections();
        if(!collections.isEmpty()) {
            StructuredQueryDefinition query = sqb.collection(collections.toArray(new String[collections.size()]));
            QueryBatcher batcher = dmm.newQueryBatcher(query);
            batcher
                .withConsistentSnapshot()
                .onUrisReady(new DeleteListener());
            dmm.startJob(batcher);
            batcher.awaitCompletion();
            dmm.stopJob(batcher);
        }
    }

    protected Properties loadProperties() {
        Properties props = new Properties();

        try (InputStream is = this.getClass().getResourceAsStream("/test.properties")) {
            props.load(is);
        } catch (Throwable t) {
        }

        try (InputStream is = this.getClass().getResourceAsStream("/test-local.properties")) {
            props.load(is);
        } catch (Throwable t) {
        }

        return props;
    }

    private HandlerProperties newHandlerProperties() {
        HandlerProperties handlerProperties = new HandlerProperties();

        Properties props = loadProperties();

        handlerProperties.setHost(props.getProperty("gg.handler.marklogic.host"));
        handlerProperties.setDatabase(props.getProperty("gg.handler.marklogic.database"));
        handlerProperties.setBinaryDatabase(props.getProperty("gg.handler.marklogic.binaryDatabase"));
        handlerProperties.setPort(props.getProperty("gg.handler.marklogic.port"));
        handlerProperties.setSsl(Boolean.parseBoolean(props.getProperty("gg.handler.marklogic.ssl")));
        handlerProperties.setGateway(Boolean.parseBoolean(props.getProperty("gg.handler.marklogic.gateway")));
        handlerProperties.setUser(props.getProperty("gg.handler.marklogic.user"));
        handlerProperties.setPassword(props.getProperty("gg.handler.marklogic.password"));
        handlerProperties.setAuth(props.getProperty("gg.handler.marklogic.auth"));
        handlerProperties.setCollections(props.getProperty("gg.handler.marklogic.collections"));

        handlerProperties.setTruststore(props.getProperty("gg.handler.marklogic.truststore"));
        handlerProperties.setTruststoreFormat(props.getProperty("gg.handler.marklogic.truststoreFormat"));
        handlerProperties.setTruststorePassword(props.getProperty("gg.handler.marklogic.truststorePassword"));

        handlerProperties.setOrg(props.getProperty("gg.handler.marklogic.org"));
        handlerProperties.setSchema(props.getProperty("gg.handler.marklogic.schema"));
        handlerProperties.setApplication(props.getProperty("gg.handler.marklogic.application"));
        handlerProperties.setImageProperty(props.getProperty("gg.handler.marklogic.imageProperty"));
        handlerProperties.setImageFormat(props.getProperty("gg.handler.marklogic.imageFormat"));
        handlerProperties.setImageCollection(props.getProperty("gg.handler.marklogic.imageCollection"));
        handlerProperties.setImageDb(props.getProperty("gg.handler.marklogic.imageDb"));
        handlerProperties.setImageKeyProps(props.getProperty("gg.handler.marklogic.imageKeyProps"));

        handlerProperties.setBatchSize(Integer.parseInt(props.getProperty("gg.handler.marklogic.batchSize")));
        handlerProperties.setThreadCount(Integer.parseInt(props.getProperty("gg.handler.marklogic.threadCount")));

        handlerProperties.setOrg(props.getProperty("gg.handler.marklogic.org"));

        handlerProperties.setTransformName(props.getProperty("gg.handler.marklogic.transformName"));
        handlerProperties.setTransformParams(props.getProperty("gg.handler.marklogic.transformParams"));

        return handlerProperties;
    }

    public DatabaseClient getDatabaseClient() {
        return databaseClient;
    }

    public DatabaseClient getBinaryDatabaseClient() {
        return binaryDatabaseClient;
    }

    public HandlerProperties getHandlerProperties() {
        return handlerProperties;
    }

    protected Map<String, Object> getInstance(Map<String, Object> document, String schemaName, String tableName) {
        Map<String, Object> envelope = (Map<String, Object>) document.get("envelope");
        if(envelope == null) {
            // this is probably an XML document
            envelope = document;
        }

        Map<String, Object> instance = (Map<String, Object>) envelope.get("instance");
        Map<String, Object> schema = (Map<String, Object>) instance.get(schemaName.toUpperCase());
        Map<String, Object> table = (Map<String, Object>) schema.get(tableName.toUpperCase());
        return table;
    }

    protected byte[] readBinary(String path) throws IOException {
        BufferedImage image = ImageIO.read(getClass().getResource(path));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(image, "jpg", baos);
        baos.flush();
        byte[] binaryImage = baos.toByteArray();
        baos.close();

        return binaryImage;
    }


}
