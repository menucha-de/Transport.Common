package havis.transport.common.osgi;

import havis.transport.common.Storage;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.Application;

class RESTApplication extends Application {

	private Set<Object> singletons = new HashSet<Object>();

	public RESTApplication() {
		singletons.add(Storage.INSTANCE);
	}

	@Override
	public Set<Object> getSingletons() {
		return singletons;
	}
}