package havis.transport.common;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.Application;

@Deprecated
/**
 * Deprecated RESTApplication, please use havis.transport.common.rest.RESTApplication
 */
public class RESTApplication extends Application {

	private Set<Object> singletons = new HashSet<Object>();

	public RESTApplication() {
		singletons.add(Storage.INSTANCE);
	}

	@Override
	public Set<Object> getSingletons() {
		return singletons;
	}
}