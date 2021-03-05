package havis.transport.common;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;

/**
 * Abstract provider factory
 */
public abstract class Provider {

	private static Provider instance;

	/**
	 * @return the current factory
	 */
	public static Provider getFactory() {
		if (instance == null)
			throw new IllegalStateException("Provider factory has not been initialized");
		return instance;
	}

	/**
	 * @param provider
	 *            the factory to set
	 */
	public static void createFactory(Provider provider) {
		if (provider == null)
			throw new NullPointerException("provider must not be null");
		instance = provider;
	}

	/**
	 * Clear the current factory, provider instantiation will not be possible
	 */
	public static void clearFactory() {
		instance = null;
	}

	public abstract <T> MessageBodyReader<T> getMessageBodyReader(Class<T> clazz, Type type, Annotation[] annotations, MediaType mediaType);

	public abstract <T> T read(MessageBodyReader<T> reader, Class<T> clazz, Type type, Annotation[] annotations, MediaType mediaType,
			MultivaluedMap<String, String> properties, InputStream stream) throws Exception;

	public abstract <T> MessageBodyWriter<T> getMessageBodyWriter(Class<T> clazz, Type type, Annotation[] annotations, MediaType mediaType);

	public abstract <T> void write(MessageBodyWriter<T> writer, T message, Class<?> clazz, Type type, Annotation[] annotations, MediaType mediaType,
			MultivaluedMap<String, Object> properties, OutputStream stream) throws Exception;
}