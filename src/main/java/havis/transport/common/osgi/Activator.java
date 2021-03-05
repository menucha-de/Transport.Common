package havis.transport.common.osgi;

import havis.transport.Transporter;
import havis.transport.ValidationException;
import havis.transport.common.Connector;
import havis.transport.common.Provider;
import havis.transport.common.Utils;
import havis.util.monitor.Broker;
import havis.util.monitor.Event;
import havis.util.monitor.Source;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Providers;
import javax.ws.rs.ext.RuntimeDelegate;

import org.jboss.resteasy.spi.ResteasyProviderFactory;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.PrototypeServiceFactory;
import org.osgi.framework.ServiceObjects;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.util.tracker.ServiceTracker;

public class Activator implements BundleActivator {

	private final static Logger log = Logger.getLogger(Activator.class.getName());
	private ServiceTracker<ClassLoader, ClassLoader> contextTracker;
	private final List<ServiceRegistration<?>> registrations = new ArrayList<>();

	@Override
	public void start(final BundleContext context) throws Exception {
		long start = System.currentTimeMillis();

		// get context class loader to load providers
		contextTracker = new ServiceTracker<ClassLoader, ClassLoader>(context, ClassLoader.class, null) {
			@Override
			public ClassLoader addingService(ServiceReference<ClassLoader> reference) {
				final ClassLoader contextClassLoader = super.addingService(reference);
				Providers providers = null;
				ClassLoader current = Thread.currentThread().getContextClassLoader();
				Thread.currentThread().setContextClassLoader(contextClassLoader);
				try {
					RuntimeDelegate delegate = RuntimeDelegate.getInstance();
					if (delegate instanceof Providers) {
						providers = (Providers) delegate;
					}
				} finally {
					Thread.currentThread().setContextClassLoader(current);
				}

				if (providers != null) {
					final Providers p = providers;
					Provider.createFactory(new Provider() {

						ThreadLocal<Providers> currentProvider = new ThreadLocal<Providers>() {
							@Override
							protected Providers initialValue() {
								// push to context once per thread
								ResteasyProviderFactory.pushContext(Providers.class, p);
								return p;
							}
						};

						@Override
						public <T> MessageBodyReader<T> getMessageBodyReader(Class<T> clazz, Type type, Annotation[] annotations, MediaType mediaType) {
							ClassLoader current = Thread.currentThread().getContextClassLoader();
							Thread.currentThread().setContextClassLoader(contextClassLoader);
							try {
								return currentProvider.get().getMessageBodyReader(clazz, type, annotations, mediaType);
							} finally {
								Thread.currentThread().setContextClassLoader(current);
							}
						}

						@Override
						public <T> T read(MessageBodyReader<T> reader, Class<T> clazz, Type type, Annotation[] annotations, MediaType mediaType,
								MultivaluedMap<String, String> properties, InputStream stream) throws Exception {
							ClassLoader current = Thread.currentThread().getContextClassLoader();
							Thread.currentThread().setContextClassLoader(contextClassLoader);
							try {
								currentProvider.get();
								return reader.readFrom(clazz, type, annotations, mediaType, properties, stream);
							} finally {
								Thread.currentThread().setContextClassLoader(current);
							}
						}

						@Override
						public <T> MessageBodyWriter<T> getMessageBodyWriter(Class<T> clazz, Type type, Annotation[] annotations, MediaType mediaType) {
							ClassLoader current = Thread.currentThread().getContextClassLoader();
							Thread.currentThread().setContextClassLoader(contextClassLoader);
							try {
								return currentProvider.get().getMessageBodyWriter(clazz, type, annotations, mediaType);
							} finally {
								Thread.currentThread().setContextClassLoader(current);
							}
						}

						@Override
						public <T> void write(MessageBodyWriter<T> writer, T data, Class<?> clazz, Type type, Annotation[] annotations, MediaType mediaType,
								MultivaluedMap<String, Object> properties, OutputStream stream) throws Exception {
							ClassLoader current = Thread.currentThread().getContextClassLoader();
							Thread.currentThread().setContextClassLoader(contextClassLoader);
							try {
								currentProvider.get();
								writer.writeTo(data, clazz, type, annotations, mediaType, properties, stream);
							} finally {
								Thread.currentThread().setContextClassLoader(current);
							}
						}

					});
				}
				return contextClassLoader;
			}

			@Override
			public void removedService(ServiceReference<ClassLoader> reference, ClassLoader service) {
				Provider.clearFactory();
				super.removedService(reference, service);
			}
		};
		contextTracker.open();

		// register internal transporters in OSGi
		for (Entry<String, Class<?>> transporter : Utils.getCommonTransporters().entrySet()) {
			register(context, transporter.getKey(), transporter.getValue());
		}

		// create connector factory to retrieve external transporters via OSGi
		Connector.createFactory(new Connector() {

			private Broker broker;
			private Lock lock = new ReentrantLock();

			@Override
			public <S> S newInstance(Class<S> clazz, String type) throws ValidationException {
				try {
					for (ServiceReference<S> reference : context.getServiceReferences(clazz, "(name=" + type + ")")) {
						ServiceObjects<S> objects = context.getServiceObjects(reference);
						if (objects != null) {
							return objects.getService();
						}
					}
					return null;
				} catch (InvalidSyntaxException e) {
					throw new ValidationException(e.getMessage());
				}
			}

			@Override
			public <S> List<String> getTypes(Class<S> clazz) throws ValidationException {
				List<String> types = new ArrayList<>();
				try {
					for (ServiceReference<S> reference : context.getServiceReferences(clazz, null)) {
						Object type = reference.getProperty("name");
						if (type instanceof String) {
							types.add((String) type);
						}
					}
				} catch (InvalidSyntaxException e) {
					throw new ValidationException(e.getMessage());
				}
				return types;
			}

			@Override
			public Broker getBroker() {
				if (broker == null) {
					lock.lock();
					try {
						if (broker == null) {
							try {
								for (ServiceReference<Broker> reference : context.getServiceReferences(Broker.class, null)) {
									ServiceObjects<Broker> objects = context.getServiceObjects(reference);
									if (objects != null) {
										broker = objects.getService();
										break;
									}
								}
							} catch (InvalidSyntaxException e) {
								// ignore
							}
						}
					} finally {
						lock.unlock();
					}
				}
				if (broker == null) {
					return new Broker() {
						@Override
						public void notify(Source arg0, Event arg1) {
						}
					};
				}
				return broker;
			}
		});

		log.log(Level.FINE, "Register application ''{0}''", RESTApplication.class.getName());
		registrations.add(context.registerService(Application.class, new RESTApplication(), null));

		log.log(Level.FINE, "Bundle start took {0}ms", String.valueOf(System.currentTimeMillis() - start));
	}

	@SuppressWarnings("rawtypes")
	private void register(final BundleContext context, final String tranporter, final Class<?> type) {
		Dictionary<String, String> properties = new Hashtable<>();
		properties.put("name", tranporter);
		log.log(Level.FINE, "Register prototype service factory {0} (''{1}'': ''{2}'')", new Object[] { type.getName(), "name", tranporter });
		registrations.add(context.registerService(Transporter.class.getName(), new PrototypeServiceFactory<Transporter>() {
			@Override
			public Transporter getService(Bundle bundle, ServiceRegistration<Transporter> registration) {
				try {
					return (Transporter) type.newInstance();
				} catch (InstantiationException | IllegalAccessException e) {
					return null;
				}
			}

			@Override
			public void ungetService(Bundle bundle, ServiceRegistration<Transporter> registration, Transporter service) {
			}
		}, properties));
	}

	@Override
	public void stop(BundleContext context) throws Exception {
		Connector.clearFactory();
		for (ServiceRegistration<?> registration : registrations)
			registration.unregister();
		registrations.clear();
		if (contextTracker != null)
			contextTracker.close();
	}
}
