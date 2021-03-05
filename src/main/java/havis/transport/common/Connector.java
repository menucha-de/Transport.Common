package havis.transport.common;

import havis.transport.ValidationException;
import havis.util.monitor.Broker;

import java.util.List;

/**
 * Abstract connector factory
 */
public abstract class Connector {

	private static Connector instance;

	/**
	 * @return the current factory
	 */
	public static Connector getFactory() {
		if (instance == null)
			throw new IllegalStateException("Connector factory has not been initialized");
		return instance;
	}

	/**
	 * @param connector
	 *            the factory to set
	 */
	public static void createFactory(Connector connector) {
		if (connector == null)
			throw new NullPointerException("connnector must not be null");
		instance = connector;
	}

	/**
	 * Clear the current factory, connector instantiation will not be possible
	 */
	public static void clearFactory() {
		instance = null;
	}

	/**
	 * Creates a new connector instance
	 * 
	 * @param clazz
	 *            the connector interface
	 * @param type
	 *            the type of the connector
	 * @return the connector instance
	 * @throws ValidationException
	 *             if creation failed
	 */
	public abstract <S> S newInstance(Class<S> clazz, String type) throws ValidationException;

	/**
	 * Get all types for the specified connector interface
	 * 
	 * @param clazz
	 *            the connector interface
	 * @return all types for the specified connector interface
	 * @throws ValidationException
	 *             if retrieval failed
	 */
	public abstract <S> List<String> getTypes(Class<S> clazz) throws ValidationException;

	/**
	 * @return the broker for monitoring
	 */
	public abstract Broker getBroker();
}