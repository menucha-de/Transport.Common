package havis.transport.common.rest;

import havis.transport.SubscriberManager;
import havis.transport.ValidationException;
import havis.transport.common.Storage;
import havis.transport.common.rest.provider.TransportExceptionMapper;
import havis.transport.common.rest.provider.ValidationExceptionMapper;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.Application;

public class RESTApplication extends Application {

	private final static String PROVIDERS = "javax.ws.rs.ext.Providers";

	private Set<Object> singletons = new HashSet<Object>();
	private Map<String, Object> properties = new HashMap<>();

	public RESTApplication() throws ValidationException {
		singletons.add(Storage.INSTANCE);
	}

	public RESTApplication(SubscriberManager subscriberManager) throws ValidationException {
		singletons.add(Storage.INSTANCE);
		singletons.add(new SubscriberManagerService(subscriberManager));
		properties.put(PROVIDERS, new Class<?>[] { ValidationExceptionMapper.class, TransportExceptionMapper.class });
	}

	@Override
	public Set<Object> getSingletons() {
		return singletons;
	}

	@Override
	public Map<String, Object> getProperties() {
		return properties;
	}

}