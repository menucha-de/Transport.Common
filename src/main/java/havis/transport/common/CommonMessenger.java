package havis.transport.common;

import havis.transform.TransformException;
import havis.transform.Transformer;
import havis.transport.Callback;
import havis.transport.CompletionHandler;
import havis.transport.FutureSendTask;
import havis.transport.MessageReceiver;
import havis.transport.Messenger;
import havis.transport.TransportConnectionException;
import havis.transport.TransportException;
import havis.transport.Transporter;
import havis.transport.ValidationException;
import havis.util.monitor.TransportError;
import havis.util.monitor.TransportQueueError;
import havis.util.monitor.TransportSource;

import java.net.URI;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.SocketFactory;

/**
 * Common messenger
 * 
 * @param <T>
 *            type of message
 */
public class CommonMessenger<T> implements Messenger<T> {

	private final static Logger log = Logger.getLogger(Messenger.class.getName());

	private URI uri;
	private Transporter<T> transporter;
	private Transformer transformer;

	private boolean errorLogging = true;

	private MessageReceiver receiver;
	private AtomicBoolean disposed = new AtomicBoolean(false);

	private int repeatPeriod = -1;
	private int queueSize = -1;
	private boolean exceeded;
	private boolean failed;
	private long lastSuccess = -1;
	private long lastError = -1;
	private AtomicBoolean connectionErrorLogged = new AtomicBoolean(false);

	long connectionErrorThresholdMs = 30 * 1000;

	private static class ErrorState {
		private AtomicBoolean first = new AtomicBoolean(true);
		private AtomicBoolean current = new AtomicBoolean(true);

		public boolean setError() {
			return (current.compareAndSet(false, true) || first.compareAndSet(true, false));
		}

		public boolean wasError() {
			return (current.compareAndSet(true, false) && !first.compareAndSet(true, false));
		}

		public boolean isError() {
			return current.get();
		}
	}
	private ErrorState errorState = new ErrorState();
	private ErrorState queueErrorState = new ErrorState();
	private ErrorState queueFilledState = new ErrorState();

	private TransportSource source = new TransportSource() {
		@Override
		public String getUri() {
			return getDisplayName();
		}
	};
	private CompletionHandler completionHandler = new CompletionHandler() {
		@Override
		public void onSuccess() {
			onTransportSuccess();
		}

		@Override
		public void onError(Throwable error) {
			onTransportError(error);
		}

		@Override
		public void onError(String errorMessage) {
			onTransportError(errorMessage, false);
		}
	};

	private ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>()) {
		@SuppressWarnings("unchecked")
		@Override
		protected <V> RunnableFuture<V> newTaskFor(Runnable runnable, V value) {
			return (RunnableFuture<V>) new FutureSendTask(runnable, (Void) value, completionHandler);
		}

		@SuppressWarnings("unchecked")
		@Override
		protected <V> RunnableFuture<V> newTaskFor(Callable<V> callable) {
			return (RunnableFuture<V>) new FutureSendTask((Callable<Void>) callable, completionHandler);
		}
	};

	/**
	 * @return whether errors will be logged
	 */
	public boolean isErrorLogging() {
		return errorLogging;
	}

	/**
	 * Set whether errors will be logged
	 * 
	 * @param errorLogging
	 *            true to log errors, false otherwise
	 */
	public void setErrorLogging(boolean errorLogging) {
		this.errorLogging = errorLogging;
	}

	@Override
	public void init(Class<T> clazz, URI uri, Map<String, String> properties) throws ValidationException {
		this.uri = Objects.requireNonNull(uri, "uri must not be null");
		transporter = getTransporter(uri.getScheme());
		if (transporter != null) {
			try {
				transporter.init(clazz, uri, properties);
			} catch (ValidationException e) {
				throw new ValidationException("Failed to initialize transporter for scheme '" + uri.getScheme() + "': " + e.getMessage());
			}
		} else {
			throw new ValidationException("Couldn't find any transporter for scheme '" + uri.getScheme() + "'");
		}
		init(properties);
	}

	private Map<String, String> init(Map<String, String> properties) throws ValidationException {
		if (properties != null) {
			for (Entry<String, String> entry : properties.entrySet()) {
				if (entry.getKey() != null) {
					switch (entry.getKey()) {
					case TRANSFORMER_PROPERTY:
						transformer = getTransformer(entry.getValue());
						if (transformer != null) {
							try {
								transformer.init(properties);
							} catch (havis.transform.ValidationException e) {
								throw new ValidationException("Failed to initialize transformer '" + entry.getValue() + "': " + e.getMessage());
							}
						} else {
							throw new ValidationException("Couldn't find transformer '" + entry.getValue() + "'");
						}
						break;
					case RESEND_REPEAT_PERIOD_PROPERTY:
						try {
							repeatPeriod = Integer.parseInt(entry.getValue());
						} catch (NumberFormatException e) {
							throw new ValidationException("Unable to parse value '" + entry.getValue() + "' for resend repeat period");
						}
						if (repeatPeriod < 1000)
							throw new ValidationException("Value '" + entry.getValue() + "' for resend repeat period must be greater or equal than 1000 ms");
						break;
					case RESEND_QUEUE_SIZE_PROPERTY:
						try {
							queueSize = Integer.parseInt(entry.getValue());
						} catch (NumberFormatException e) {
							throw new ValidationException("Unable to parse value '" + entry.getValue() + "' for resend queue size");
						}
						if (queueSize < 1)
							throw new ValidationException("Value '" + entry.getValue() + "' for resend queue size must be greater than zero");
						break;
					}
				}
			}
		}
		return properties;
	}

	@Override
	public void init(Class<T> clazz, MessageReceiver callback, Map<String, String> properties) throws ValidationException {
		this.receiver = Objects.requireNonNull(callback, "callback must not be null");
		this.transporter = new Transporter<T>() {
			@Override
			public void init(Class<T> clazz, URI uri, Map<String, String> properties) throws ValidationException {
			}

			@Override
			public boolean supportsSocketFactory() {
				return false;
			}

			@Override
			public void setSocketFactory(SocketFactory socketFactory) throws TransportException {
				throw new TransportException("Transporter does not support certificates");
			}

			@Override
			public void send(Object message) throws TransportException {
				if (CommonMessenger.this.receiver != null) {
					MessageReceiver c = CommonMessenger.this.receiver;
					CommonMessenger.this.receiver = null;
					c.receive(message);
					// dispose listeners immediately after sending
					CommonMessenger.this.dispose();
				}
			}

			@Override
			public void send(Object message, String name, String path, Map<String, String> properties) throws TransportException {
				throw new TransportException("Listener does not support sending with additonal information");
			}

			@Override
			public void addPath(String path, Callback callback) throws TransportException {
				throw new TransportException("Transporter does not support receiving");
			}

			@Override
			public void removePath(String path, Callback callback) throws TransportException {
				throw new TransportException("Transporter does not support receiving");
			}

			@Override
			public void dispose() {
			}
		};
		init(properties);
	}
	
	private void onTransportError(Throwable error) {
		onTransportError(error != null ? error.getMessage() : null, error instanceof TransportConnectionException);
	}
	
	private void onTransportError(String errorMessage, boolean isConnectionError) {
		if (errorMessage != null && errorState.setError()) {
			Connector.getFactory().getBroker().notify(source, new TransportError(new Date(), true, "Transport failed: " + errorMessage));
			if (errorLogging)
				log.log(isConnectionError && lastSuccess > -1 ? Level.WARNING : Level.SEVERE, "Transport {0} delivery entered error state: {1}", new Object[] { getDisplayName(), errorMessage });
		}
	}
	
	private void onTransportSuccess() {
		if (errorState.wasError()) {
			String additionalMessage = "";
			if (repeatPeriod > 0) {
				int remainingMessagesInQueue = executor.getQueue().size();
				if (remainingMessagesInQueue > 0)
					additionalMessage = " (queue size: " + remainingMessagesInQueue + ")";
				else
					additionalMessage = " (queue is empty)";
			}
			Connector.getFactory().getBroker().notify(source, new TransportError(new Date(), false, "Transport error resolved" + additionalMessage));
			if (errorLogging)
				log.log(Level.INFO, "Transport {0} delivery error state was resolved" + additionalMessage, getDisplayName());
		}
	}
	
	private void onTransportQueueError(String errorMessage) {
		if (errorMessage != null && queueErrorState.setError()) {
			Connector.getFactory().getBroker().notify(source, new TransportQueueError(new Date(), true, "Transport queue full: " + errorMessage));
			if (errorLogging)
				log.log(Level.SEVERE, "Transport {0} queue is full: {1}", new Object[] { getDisplayName(), errorMessage });
		}
	}
	
	private void onTransportQueueSuccess() {
		if (queueErrorState.wasError()) {
			Connector.getFactory().getBroker().notify(source, new TransportQueueError(new Date(), false, "Transport queue not full anymore"));
			if (errorLogging)
				log.log(Level.INFO, "Transport {0} queue is not full anymore", getDisplayName());
		}
	}

	private void onTransportQueueIncreased() {
		log.log(Level.FINE, "Transport {0} queue size is currently " + executor.getQueue().size(), getDisplayName());
		queueFilledState.setError();
	}

	private void onTransportQueueDecreased() {
		int remainingMessagesInQueue = executor.getQueue().size();
		log.log(Level.FINE, "Transport {0} queue size is currently " + remainingMessagesInQueue, getDisplayName());
		if (remainingMessagesInQueue == 0 && queueFilledState.wasError()) {
			Connector.getFactory().getBroker().notify(source, new TransportQueueError(new Date(), false, "Transport queue is now empty"));
			if (errorLogging)
				log.log(Level.INFO, "Transport {0} queue is now empty", getDisplayName());
		}
	}

	private String getDisplayName() {
		return uri != null ? uri.toString() : "listener";
	}

	private Transporter<T> getTransporter(String transporter) throws ValidationException {
		return Utils.getTransporter(transporter);
	}

	private Transformer getTransformer(String transformer) throws ValidationException {
		if (transformer != null && transformer.length() > 0) {
			Transformer instance = Connector.getFactory().newInstance(Transformer.class, transformer);
			return instance;
		}
		return null;
	}

	private void onTransportConnectionError(boolean error) {
		if (error) {
			long now = System.currentTimeMillis();
			if (lastError > -1 && lastSuccess > -1 && now - lastSuccess > connectionErrorThresholdMs && !connectionErrorLogged.getAndSet(true)) {
				log.log(Level.SEVERE,
						"Transport {0} delivery failed multiple times, the connection has been interrupted for more than {1} seconds",
						new Object[] { getDisplayName(), connectionErrorThresholdMs / 1000 });
			}
			lastError = now;
		} else {
			lastSuccess = System.currentTimeMillis();
			lastError = -1;
			connectionErrorLogged.set(false);
		}
	}

	protected FutureSendTask send(final T message, final String name, final String path, final Map<String, String> properties) {
		if (queueSize > 0 && executor.getQueue().size() == queueSize) {
			if (!exceeded) {
				exceeded = true;
				onTransportQueueError("Exceeded maximum number of pending messages, messages will be discarded.");
			}
			return new FutureSendTask(new Runnable() {
				@Override
				public void run() {
				}
			}, null, null) {
				{
					run();
				}
			};
		}
		if (exceeded) {
			exceeded = false;
			onTransportQueueSuccess();
		}
		return (FutureSendTask) executor.submit(new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				if (message == null)
					throw new TransportException("message must not be null");

				Object msg = message;
				if (transformer != null) {
					try {
						msg = transformer.transform(message);
					} catch (TransformException e) {
						throw new TransportException("Transformation for \"" + getDisplayName() + "\" failed: " + e.getMessage());
					}
					if (msg == null)
						throw new TransportException("Transformation for \"" + getDisplayName() + "\" failed: message is null after transformation");
				}
				
				while (true) {
					try {
						if (path != null)
							transporter.send(msg, name, path, properties);
						else
							transporter.send(msg);
						onTransportConnectionError(false);
					} catch (TransportConnectionException e) {
						if (repeatPeriod > 0) {
							onTransportQueueIncreased();
							if (!failed) {
								failed = true;
								onTransportError(e);
							}
							onTransportConnectionError(true);
							Thread.sleep(repeatPeriod);
							continue;
						} else {
							onTransportConnectionError(true);
							throw e;
						}
					}
					if (failed) {
						failed = false;
						onTransportSuccess();
					}
					if (repeatPeriod > 0) {
						onTransportQueueDecreased();
					}
					break;
				}
				return null;
			}
		});
	}

	@Override
	public FutureSendTask send(final T message) {
		return send(message, null, null, null);
	}

	@Override
	public boolean isErrorState() {
		return this.errorState.isError() || this.queueErrorState.isError();
	}

	@Override
	public URI getUri() {
		return uri;
	}

	@Override
	public void addPath(String path, Callback callback) throws TransportException {
		if (transporter != null)
			transporter.addPath(path, callback);
	}

	@Override
	public void removePath(String path, Callback callback) throws TransportException {
		if (transporter != null)
			transporter.removePath(path, callback);
	}

	@Override
	public void dispose() {
		if (!disposed.getAndSet(true)) {
			executor.shutdownNow();
			if (receiver != null) {
				try {
					receiver.cancel();
				} catch (NullPointerException e) {
					// race condition, ignore
				}
			}
			if (transporter != null) {
				transporter.dispose();
			}
		}
	}

	@Override
	public boolean supportsSocketFactory() {
		if (this.transporter != null) {
			return this.transporter.supportsSocketFactory();
		}
		return false;
	}

	@Override
	public void setSocketFactory(SocketFactory socketFactory) throws TransportException {
		if (this.transporter != null) {
			this.transporter.setSocketFactory(socketFactory);
		}
	}
}