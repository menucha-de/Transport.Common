package havis.transport.common;

import havis.transport.Callback;
import havis.transport.DataWriter;
import havis.transport.TransportException;
import havis.transport.Transporter;
import havis.transport.ValidationException;

import java.net.URI;
import java.util.List;
import java.util.Map;

import javax.net.SocketFactory;

/**
 * Base class for all transporters using a data writer
 * 
 * @param <T>
 *            type of messages
 */
public abstract class PlainTransporter<T> implements Transporter<T>, DataWriter {

	private PlainDataConverter converter = new PlainDataConverter();

	@Override
	public final void init(Class<T> clazz, URI uri, Map<String, String> properties) throws ValidationException {
		converter.init(properties);
		init(uri, properties, converter.getFields());
	}

	/**
	 * Initialize the transporter
	 * 
	 * @param uri
	 *            the URI to send to
	 * @param properties
	 *            the properties containing settings for transport
	 * @throws ValidationException
	 *             if arguments are not valid
	 */
	protected abstract void init(URI uri, Map<String, String> properties, List<String> fields) throws ValidationException;

	@Override
	public boolean supportsSocketFactory() {
		return false;
	}

	@Override
	public void setSocketFactory(SocketFactory socketFactory) throws TransportException {
		throw new TransportException("Transporter does not support certificates");
	}

	@Override
	public final void send(Object message) throws TransportException {
		converter.convert(message, this);
	}

	@Override
	public void send(Object message, String name, String path, Map<String, String> properties) throws TransportException {
		// TODO: properties?
		if (path != null && path.length() > 0)
			throw new TransportException("Transporter does not support paths");
		send(message);
	}

	@Override
	public void addPath(String path, Callback callback) throws TransportException {
		throw new TransportException("Transporter does not support receiving");
	}

	@Override
	public void removePath(String path, Callback callback) throws TransportException {
		throw new TransportException("Transporter does not support receiving");
	}
}