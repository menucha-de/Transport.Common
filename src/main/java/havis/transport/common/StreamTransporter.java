package havis.transport.common;

import havis.transport.Callback;
import havis.transport.Marshaller;
import havis.transport.TransportException;
import havis.transport.Transporter;
import havis.transport.ValidationException;

import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.SocketFactory;

/**
 * Base class for all transporters using a marshaller to stream data
 * 
 * @param <T>
 *            type of messages
 */
public abstract class StreamTransporter<T> implements Transporter<T> {

	private final static Logger log = Logger.getLogger(StreamTransporter.class.getName());

	private final StreamFactory<T> writer = new StreamFactory<>();
	private Map<String, List<Callback>> callbacks = new HashMap<>();
	private Lock lock = new ReentrantLock();

	@Override
	public final void init(Class<T> clazz, URI uri, Map<String, String> properties) throws ValidationException {
		writer.init(clazz, getDefaultMimeType(), properties);
		init(uri, properties);
	}

	@Override
	public boolean supportsSocketFactory() {
		return false;
	}

	@Override
	public void setSocketFactory(SocketFactory socketFactory) throws TransportException {
		throw new TransportException("Transporter does not support certificates");
	}

	@SuppressWarnings("unchecked")
	@Override
	public final void send(Object message) throws TransportException {
		send(writer.getMarshaller(), (T) message);
	}

	@SuppressWarnings("unchecked")
	@Override
	public final void send(Object message, String name, String path, Map<String, String> properties) throws TransportException {
		send(writer.getMarshaller(), (T) message, name, path, properties);
	}

	/**
	 * @return the default MIME type to use for this stream transporter if no
	 *         other MIME type is specified in the properties
	 */
	protected abstract String getDefaultMimeType();

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
	protected abstract void init(URI uri, Map<String, String> properties) throws ValidationException;

	/**
	 * Send the data using the marshaller
	 * 
	 * @param marshaller
	 *            the marshaller to use
	 * @throws TransportException
	 *             if transport fails
	 */
	protected abstract void send(Marshaller<T> marshaller, T message) throws TransportException;

	/**
	 * Send the data using the marshaller with additional information
	 * 
	 * @param marshaller
	 *            the marshaller to use
	 * @param name
	 *            the name
	 * @param path
	 *            the path which extends the URI
	 * @param properties
	 *            the properties containing additional settings for transport
	 * @throws TransportException
	 *             if transport fails
	 */
	protected void send(Marshaller<T> marshaller, T message, String name, String path, Map<String, String> properties) throws TransportException {
		// TODO: properties?
		if (path != null && path.length() > 0)
			throw new TransportException("Transporter does not support paths");
		send(marshaller, message);
	}

	protected void subscribe(String path, StreamCallback callback) throws TransportException {
		throw new TransportException("Transporter does not support receiving");
	}

	protected void unsubscribe(String path) throws TransportException {
		throw new TransportException("Transporter does not support receiving");
	}

	@Override
	public void addPath(String path, final Callback callback) throws TransportException {
		lock.lock();
		try {
			List<Callback> callbacks = this.callbacks.get(path);
			if (callbacks == null) {

				subscribe(path, new StreamCallback() {
					@Override
					public void arrived(String path, InputStream stream) {
						Marshaller<T> marshaller = writer.getMarshaller();
						try {
							T message = marshaller.unmarshal(stream);
							List<Callback> currentCallbacks = StreamTransporter.this.callbacks.get(path);
							if (currentCallbacks != null)
								for (Callback callback : currentCallbacks)
									callback.arrived(path, message);
						} catch (TransportException e) {
							log.log(Level.SEVERE, "Failed to read message body", e);
						}
					}
				});
				callbacks = new CopyOnWriteArrayList<Callback>();
				this.callbacks.put(path, callbacks);
			}
			callbacks.add(callback);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void removePath(String path, final Callback callback) throws TransportException {
		lock.lock();
		try {
			List<Callback> callbacks = this.callbacks.get(path);
			if (callbacks != null) {
				callbacks.remove(callback);
				if (callbacks.isEmpty()) {
					this.callbacks.remove(path);
					unsubscribe(path);
				}
			}
		} finally {
			lock.unlock();
		}
	}
}