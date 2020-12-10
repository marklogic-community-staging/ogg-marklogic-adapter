package oracle.goldengate.delivery.handler.marklogic;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import oracle.goldengate.delivery.handler.marklogic.util.URLFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;

public class MarkLogicClientFactory {

    public static X509TrustManager getDefaultTrustManager() throws NoSuchAlgorithmException, KeyStoreException {
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init((KeyStore) null);
        for (TrustManager trustManager : trustManagerFactory.getTrustManagers()) {
            if (trustManager instanceof X509TrustManager) {
                return (X509TrustManager) trustManager;
            }
        }
        return null;
    }

    public static X509TrustManager getTrustManager(String truststoreLocation, String truststoreFormat, String truststorePassword) throws NoSuchAlgorithmException, KeyStoreException, IOException, CertificateException {
        KeyStore truststore = KeyStore.getInstance(truststoreFormat);

        try (InputStream truststoreInput = URLFactory.newURL(truststoreLocation).openStream()) {
            truststore.load(truststoreInput, truststorePassword.toCharArray());
        }

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(truststore);

        for (TrustManager trustManager : trustManagerFactory.getTrustManagers()) {
            if (trustManager instanceof X509TrustManager) {
                return (X509TrustManager) trustManager;
            }
        }

        return null;
    }

    public static DatabaseClient createClient(HandlerProperties handlerProperties, String database) throws Exception {
        DatabaseClientFactory.SecurityContext markLogicSecurityContext = handlerProperties.getAuth().equalsIgnoreCase("DIGEST") ?
            new DatabaseClientFactory.DigestAuthContext(handlerProperties.getUser(), handlerProperties.getPassword()) :
            new DatabaseClientFactory.BasicAuthContext(handlerProperties.getUser(), handlerProperties.getPassword());

        if (handlerProperties.isSsl()) {
            X509TrustManager trustManager = (handlerProperties.getTruststore() == null) ?
                getDefaultTrustManager() :
                getTrustManager(handlerProperties.getTruststore(), handlerProperties.getTruststoreFormat(), handlerProperties.getTruststorePassword());
            TrustManager[] trustManagers = {trustManager};

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustManagers, new SecureRandom());

            markLogicSecurityContext = markLogicSecurityContext
                .withSSLContext(sslContext, trustManager)
                .withSSLHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.COMMON);
        }

        DatabaseClient client = DatabaseClientFactory.newClient(
            handlerProperties.getHost(),
            Integer.parseInt(handlerProperties.getPort()),
            database,
            markLogicSecurityContext,
            handlerProperties.isGateway() ? DatabaseClient.ConnectionType.GATEWAY : DatabaseClient.ConnectionType.DIRECT
        );

        return client;
    }

    public static DatabaseClient newClient(HandlerProperties handlerProperties) throws Exception {
        return createClient(handlerProperties, handlerProperties.getDatabase());
    }

    public static DatabaseClient newBinaryClient(HandlerProperties handlerProperties) throws Exception {
        String database = handlerProperties.getBinaryDatabase();
        if(database == null) {
            database = handlerProperties.getDatabase();
        }
        return createClient(handlerProperties, database);
    }
}
