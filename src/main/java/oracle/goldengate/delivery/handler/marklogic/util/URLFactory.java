package oracle.goldengate.delivery.handler.marklogic.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLStreamHandler;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * See https://www.infoworld.com/article/2077397/urls-smart-resource-identifiers.html
 */
public abstract class URLFactory {
    public static final String HANDLER_MAPPING_RESOURCE_NAME = "url.properties";

    /**
     * A factory method for constructing a java.net.URL out of a plain string.
     * Stock URL schemes will be tried first, followed by custom schemes with
     * a mapping in the union of all {@link #HANDLER_MAPPING_RESOURCE_NAME}
     * resources found at the time this class is initialized.
     *
     * @param url external URL form [may not be null].
     * @return java.net.URL corresponding to the scheme in 'url'.
     * @throws MalformedURLException if 'url' does not correspond to a known
     *                               stock or custom URL scheme.
     */
    public static URL newURL(final String url)
        throws MalformedURLException {
        // Try already installed protocols first:
        try {
            return new URL(url);
        } catch (MalformedURLException ignore) {
            // Ignore: try our handler list next.
        }

        final int firstColon = url.indexOf(':');
        if (firstColon <= 0) {
            throw new MalformedURLException("no protocol specified: " + url);
        } else {
            final Map handlerMap = getHandlerMap();

            final String protocol = url.substring(0, firstColon);
            final URLStreamHandler handler;

            if ((handlerMap == null) ||
                (handler = (URLStreamHandler) handlerMap.get(protocol)) == null)
                throw new MalformedURLException("unknown protocol: " + protocol);


            return new URL(null, url, handler);
        }
    }


    /**
     * Not synchronized by design. You will need to change this if you make
     * HANDLERS initialize lazily (post static initialization time).
     *
     * @return scheme/stream handler map [can be null if static init failed].
     */
    private static Map /* String->URLContentHandler */ getHandlerMap() {
        return HANDLERS;
    }

    /*
     * Loads a scheme/handler map that is a union of *all* resources named
     * 'resourceName' as seen by 'loader'. Null 'loader' is equivalent to the
     * application loader.
     */
    private static Map loadHandlerList(final String resourceName,
                                       ClassLoader loader) {
        if (loader == null) loader = ClassLoader.getSystemClassLoader();

        final Map result = new HashMap();

        try {
            // NOTE: using getResources() here.
            final Enumeration resources = loader.getResources(resourceName);

            if (resources != null) {
                // Merge all mappings in 'resources':

                while (resources.hasMoreElements()) {
                    final URL url = (URL) resources.nextElement();
                    final Properties mapping;

                    InputStream urlIn = null;
                    try {
                        urlIn = url.openStream();

                        mapping = new Properties();
                        mapping.load(urlIn); // Load in .properties format.
                    } catch (IOException ioe) {
                        // Ignore this resource and go to the next one.
                        continue;
                    } finally {
                        if (urlIn != null) try {
                            urlIn.close();
                        } catch (Exception ignore) {
                        }
                    }

                    // Load all handlers specified in 'mapping':

                    for (Enumeration keys = mapping.propertyNames();
                         keys.hasMoreElements(); ) {
                        final String protocol = (String) keys.nextElement();
                        final String implClassName = mapping.getProperty(protocol);

                        final Object currentImpl = result.get(protocol);
                        if (currentImpl != null) {
                            if (implClassName.equals(currentImpl.getClass().getName()))
                                continue; // Skip duplicate mapping.
                            else
                                throw new IllegalStateException("duplicate " +
                                    "protocol handler class [" + implClassName +
                                    "] for protocol " + protocol);
                        }

                        result.put(protocol,
                            loadURLStreamHandler(implClassName, loader));
                    }
                }
            }
        } catch (IOException ignore) {
            // Ignore: an empty result will be returned.
        }

        return result;
    }

    /*
     * Loads and initializes a single URL stream handler for a given name via a
     * given classloader. For simplicity, all errors are converted to
     * RuntimeExceptions.
     */
    private static URLStreamHandler loadURLStreamHandler(final String className,
                                                         final ClassLoader loader) {
        final Class cls;
        final Object handler;
        try {
            cls = Class.forName(className, true, loader);
            handler = cls.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("could not load and instantiate" +
                " [" + className + "]: " + e.getMessage());
        }

        if (!(handler instanceof URLStreamHandler))
            throw new RuntimeException("not a java.net.URLStreamHandler" +
                " implementation: " + cls.getName());

        return (URLStreamHandler) handler;
    }

    /**
     * This method decides which classloader will be used by all
     * resource/classloading in this class. At the very least, you should use the current thread's
     * context loader. A better strategy is to use techniques shown in
     * http://www.javaworld.com/javaworld/javaqa/2003-06/01-qa-0606-load.html.
     */
    private static ClassLoader getClassLoader() {
        return Thread.currentThread().getContextClassLoader();
    }


    private static final Map /* String->URLContentHandler */ HANDLERS;
    private static final boolean DEBUG = true;

    static {
        Map temp = null;
        try {
            temp = loadHandlerList(HANDLER_MAPPING_RESOURCE_NAME,
                getClassLoader());
        } catch (Exception e) {
            if (DEBUG) {
                System.out.println("could not load all" +
                    " [" + HANDLER_MAPPING_RESOURCE_NAME + "] mappings:");
                e.printStackTrace(System.out);
            }
        }

        HANDLERS = temp;
    }
} // End of class.
