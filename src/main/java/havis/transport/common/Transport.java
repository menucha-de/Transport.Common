package havis.transport.common;

import havis.transform.TransformerFactory;
import havis.transport.ValidationException;
import havis.util.monitor.Broker;
import havis.util.monitor.Event;
import havis.util.monitor.Source;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.logging.Logger;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Providers;
import javax.ws.rs.ext.RuntimeDelegate;

import org.jboss.resteasy.spi.ResteasyProviderFactory;

public class Transport {

	private final static Logger log = Logger.getLogger(Transport.class.getName());

	public static void init() {
		init(new Broker() {
			@Override
			public void notify(Source source, Event event) {
			}
		});
	}

	public static void init(final Broker broker) {
		final Providers providers = (Providers) RuntimeDelegate.getInstance();
		Provider.createFactory(new Provider() {
			@Override
			public <T> MessageBodyReader<T> getMessageBodyReader(Class<T> clazz, Type type, Annotation[] annotations, MediaType mediaType) {
				ResteasyProviderFactory.pushContext(Providers.class, providers);
				return providers.getMessageBodyReader(clazz, type, annotations, mediaType);
			}

			@Override
			public <T> T read(MessageBodyReader<T> reader, Class<T> clazz, Type type, Annotation[] annotations, MediaType mediaType,
					MultivaluedMap<String, String> properties, InputStream stream) throws Exception {
				ResteasyProviderFactory.pushContext(Providers.class, providers);
				return reader.readFrom(clazz, type, annotations, mediaType, properties, stream);
			}

			@Override
			public <T> MessageBodyWriter<T> getMessageBodyWriter(Class<T> clazz, Type type, Annotation[] annotations, MediaType mediaType) {
				ResteasyProviderFactory.pushContext(Providers.class, providers);
				return providers.getMessageBodyWriter(clazz, type, annotations, mediaType);
			}

			@Override
			public <T> void write(MessageBodyWriter<T> writer, T data, Class<?> clazz, Type type, Annotation[] annotations, MediaType mediaType,
					MultivaluedMap<String, Object> properties, OutputStream stream) throws Exception {
				ResteasyProviderFactory.pushContext(Providers.class, providers);
				writer.writeTo(data, clazz, type, annotations, mediaType, properties, stream);
			}
		});

		Connector.createFactory(new Connector() {
			@SuppressWarnings("unchecked")
			@Override
			public <S> S newInstance(Class<S> clazz, String name) throws ValidationException {
				log.info("Instantiating transform factory");
				ServiceLoader<TransformerFactory> adapters = ServiceLoader.load(TransformerFactory.class);
				for (TransformerFactory adapter : adapters) {
					return (S) adapter.newInstance();
				}
				return null;
			}

			@Override
			public <S> List<String> getTypes(Class<S> arg0) throws ValidationException {
				return new ArrayList<String>(Utils.getCommonTransporters().keySet());
			}

			@Override
			public Broker getBroker() {
				return broker;
			}
		});
	}
}
