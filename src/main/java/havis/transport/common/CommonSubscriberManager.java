package havis.transport.common;

import havis.transport.FutureSendTask;
import havis.transport.MessageReceiver;
import havis.transport.Messenger;
import havis.transport.Subscriber;
import havis.transport.SubscriberFilter;
import havis.transport.SubscriberListener;
import havis.transport.SubscriberManager;
import havis.transport.Subscription;
import havis.transport.ValidationException;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manager for subscribers
 */
public class CommonSubscriberManager extends TaskHandler implements SubscriberManager {

	protected final static Logger log = Logger.getLogger(CommonSubscriberManager.class.getName());

	protected Lock lock = new ReentrantLock();

	protected Collection<Subscriber> subscribers;

	protected Class<?> messengerClazz;

	protected Map<String, String> defaultProperties;

	@SuppressWarnings("rawtypes")
	protected Map<Subscription, CommonMessenger> messengers = new LinkedHashMap<>();
	Map<Subscription, AtomicInteger> locks = new HashMap<>();
	Map<Subscription, AtomicInteger> uses = new HashMap<>();

	protected boolean hasEnabledSubscribers;

	protected boolean hasListeners;

	/**
	 * Creates a new subscriber manager
	 * 
	 * @param clazz
	 *            the class to use to initialize the messengers, usually the
	 *            type of messages to be send
	 * @param subscribers
	 *            the reference to the list of subscribers, will be manipulated
	 *            internally
	 * @throws ValidationException
	 *             if validation of subscribers failed
	 */
	public CommonSubscriberManager(Class<?> clazz, Collection<Subscriber> subscribers) throws ValidationException {
		this.subscribers = Objects.requireNonNull(subscribers, "subscribers must not be null");
		this.messengerClazz = Objects.requireNonNull(clazz, "clazz must not be null");
		init();
	}

	/**
	 * Creates a new subscriber manager
	 * 
	 * @param clazz
	 *            the class to use to initialize the messengers, usually the
	 *            type of messages to be send
	 * @param subscribers
	 *            the reference to the list of subscribers, will be manipulated
	 *            internally
	 * @param defaultProperties
	 *            the properties which will be set on all messengers created
	 *            internally<br>
	 *            <br>
	 *            If a subscriber specifies values for properties provided here,
	 *            the specified default value of the property will be
	 *            overwritten.<br>
	 *            <br>
	 *            specific property &gt; default property
	 * @throws ValidationException
	 *             if validation of subscribers failed
	 */
	public CommonSubscriberManager(Class<?> clazz, Collection<Subscriber> subscribers, Map<String, String> defaultProperties) throws ValidationException {
		this.subscribers = Objects.requireNonNull(subscribers, "subscribers must not be null");
		this.messengerClazz = Objects.requireNonNull(clazz, "clazz must not be null");
		this.defaultProperties = defaultProperties;
		init();
	}

	private void init() throws ValidationException {
		Set<String> ids = new HashSet<>();
		for (Subscriber s : this.subscribers) {
			if (s.getId() == null)
				throw new IllegalArgumentException("subscriber ID must be set");
			if (ids.contains(s.getId()))
				throw new IllegalArgumentException("subscriber ID must be unique");
			ids.add(s.getId());
		}
		for (Subscriber s : this.subscribers) {
			if (s.isEnable()) {
				try {
					setMessenger(s, create(s));
				} catch (ValidationException e) {
					log.log(Level.SEVERE, e.getMessage(), e);
					s.setEnable(false);
				}
			}
			this.locks.put(s, new AtomicInteger());
			this.uses.put(s, new AtomicInteger());
		}
	}

	void lock(Subscriber subscriber) {
		locks.get(subscriber).incrementAndGet();
	}

	void unlock(Subscriber subscriber) {
		locks.get(subscriber).decrementAndGet();
	}

	boolean isLocked(Subscriber subscriber) {
		return locks.get(subscriber).get() > 0;
	}

	void use(Subscriber subscriber) {
		uses.get(subscriber).incrementAndGet();
	}

	void unuse(Subscriber subscriber) {
		uses.get(subscriber).decrementAndGet();
	}

	boolean isUsed(Subscriber subscriber) {
		return uses.get(subscriber).get() > 0;
	}

	@SuppressWarnings("rawtypes")
	protected CommonMessenger getMessenger(Subscriber subscriber) {
		return this.messengers.get(subscriber);
	}

	@SuppressWarnings("rawtypes")
	protected Map<Subscription, CommonMessenger> getMessengersSnapshot() {
		return new LinkedHashMap<>(this.messengers);
	}

	private void setMessenger(Subscriber subscriber, @SuppressWarnings("rawtypes") CommonMessenger messenger) throws ValidationException {
		this.messengers.put(new Subscriber(subscriber), messenger);
		this.hasEnabledSubscribers = true;
	}

	private void setMessenger(SubscriberListener listener, @SuppressWarnings("rawtypes") CommonMessenger messenger) throws ValidationException {
		this.messengers.put(listener, messenger);
		this.hasEnabledSubscribers = true;
		this.hasListeners = true;
	}

	@SuppressWarnings("rawtypes")
	private CommonMessenger removeMessenger(Subscriber subscriber) {
		CommonMessenger remove = this.messengers.remove(subscriber);
		this.hasEnabledSubscribers = !this.messengers.isEmpty();
		return remove;
	}

	@SuppressWarnings("rawtypes")
	protected void clearListenerMessengers() {
		for (Iterator<Map.Entry<Subscription, CommonMessenger>> it = this.messengers.entrySet().iterator(); it.hasNext();) {
			if (it.next().getKey() instanceof SubscriberListener) {
				it.remove();
			}
		}
		this.hasEnabledSubscribers = !this.messengers.isEmpty();
		this.hasListeners = false;
	}

	@SuppressWarnings("rawtypes")
	private void clearMessengersExcept(Map<Subscription, CommonMessenger> except) {
		boolean listeners = false;
		for (Iterator<Entry<Subscription, CommonMessenger>> it = this.messengers.entrySet().iterator(); it.hasNext();) {
			Entry<Subscription, CommonMessenger> entry = it.next();
			if (!except.containsKey(entry.getKey()))
				it.remove();
			else if (entry.getKey() instanceof SubscriberListener)
				listeners = true;
		}
		this.hasEnabledSubscribers = !this.messengers.isEmpty();
		this.hasListeners = listeners;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private CommonMessenger create(Subscription subscription) throws ValidationException {
		CommonMessenger messenger = createMessenger();
		try {
			if (subscription instanceof Subscriber) {
				messenger.init(this.messengerClazz, new URI(((Subscriber) subscription).getUri()), getMessengerProperties(subscription));
				// TODO context protocol (SSL, TLS etc.)
				if (messenger.supportsSocketFactory()) {
					messenger.setSocketFactory(SSLContextManager.createSSLSocketFactory(subscription.getId(), "TLSv1.2"));
				}
			} else if (subscription instanceof SubscriberListener) {
				messenger.init(this.messengerClazz, (MessageReceiver) subscription, getMessengerProperties(subscription));
			} else
				throw new IllegalArgumentException("Unkown subscription type");
			return messenger;
		} catch (Exception e) {
			log.log(Level.SEVERE, "Failed to create subscriber: " + e.toString(), e);
			throw new ValidationException("Failed to create subscriber: " + e.toString());
		}
	}

	@SuppressWarnings({ "rawtypes" })
	CommonMessenger createMessenger() {
		return new CommonMessenger();
	}

	/**
	 * Get the properties to use for the messenger
	 * 
	 * @param subscription
	 *            the subscription to get the properties from
	 * @return the properties
	 */
	protected Map<String, String> getMessengerProperties(Subscription subscription) {
		Map<String, String> result = subscription.getProperties();
		if (this.defaultProperties != null) {
			result = new LinkedHashMap<>();
			result.putAll(this.defaultProperties);
			if (subscription.getProperties() != null)
				result.putAll(subscription.getProperties());
		}
		return result;
	}

	/**
	 * Add a subscriber, ID will be generated
	 * 
	 * @param subscriber
	 *            the subscriber without and ID (will be generated)
	 * @return the generated ID
	 * @throws ValidationException
	 *             if validation of subscriber failed
	 */
	@Override
	public String add(Subscriber subscriber) throws ValidationException {
		Objects.requireNonNull(subscriber, "subscriber must not be null");
		if (subscriber.getId() != null)
			throw new IllegalArgumentException("subscriber ID must not be set");
		subscriber.setId(UUID.randomUUID().toString());

		@SuppressWarnings("rawtypes")
		CommonMessenger messenger = null;
		if (subscriber.isEnable()) {
			messenger = create(subscriber);
		}

		this.lock.lock();
		try {
			if (messenger != null)
				setMessenger(subscriber, messenger);
			this.subscribers.add(subscriber);
			this.locks.put(subscriber, new AtomicInteger());
			this.uses.put(subscriber, new AtomicInteger());
		} finally {
			this.lock.unlock();
		}
		return subscriber.getId();
	}

	/**
	 * Add a subscriber listener. The listener will be removed automatically
	 * after a single message was received.
	 * 
	 * @param listener
	 *            the subscriber listener to add
	 * @throws ValidationException
	 *             if validation of subscriber failed
	 */
	@Override
	public void add(SubscriberListener listener) throws ValidationException {
		Objects.requireNonNull(listener, "listener must not be null");
		@SuppressWarnings("rawtypes")
		CommonMessenger messenger = create(listener);
		this.lock.lock();
		try {
			setMessenger(listener, messenger);
		} finally {
			this.lock.unlock();
		}
	}

	/**
	 * Update a subscriber, ID must be specified
	 * 
	 * @param subscriber
	 *            the subscriber to update
	 * @throws ValidationException
	 *             if validation of subscriber failed
	 */
	@Override
	public void update(Subscriber subscriber) throws ValidationException {
		Objects.requireNonNull(subscriber, "subscriber must not be null");
		if (subscriber.getId() == null)
			throw new IllegalArgumentException("subscriber ID must be set");

		@SuppressWarnings("rawtypes")
		CommonMessenger messenger = null;
		if (subscriber.isEnable()) {
			messenger = create(subscriber);
		}

		@SuppressWarnings("rawtypes")
		CommonMessenger oldMessenger = null;

		boolean success = false;

		this.lock.lock();
		try {
			Subscriber current = null;
			for (Subscriber s : this.subscribers) {
				if (subscriber.equals(s)) {
					current = s;
					break;
				}
			}
			if (current == null)
				throw new IllegalArgumentException("subscriber with ID " + subscriber.getId() + " was not found");

			if (isUsed(current))
				throw new ValidationException("Could not update an used subscriber");

			if (subscriber.isEnable()) {
				// will be enabled now
				// might be null if it was disabled before
				oldMessenger = getMessenger(current);
			} else {
				// will be disabled
				oldMessenger = removeMessenger(current);
			}
			current.set(subscriber);

			if (messenger != null)
				setMessenger(current, messenger);

			success = true;
		} finally {
			this.lock.unlock();

			if (!success && messenger != null)
				messenger.dispose();
		}

		if (oldMessenger != null) {
			oldMessenger.dispose();
			cancelTasksFor(subscriber);
		}
	}

	/**
	 * Remove a subscriber by ID
	 * 
	 * @param id
	 *            the ID of the subscriber to remove
	 */
	@Override
	public void remove(String id) throws ValidationException {
		Objects.requireNonNull(id, "ID must not be null");
		remove(new Subscriber(id));
	}

	/**
	 * Remove a subscriber, ID must be specified
	 * 
	 * @param subscriber
	 *            the subscriber to remove
	 */
	@Override
	public void remove(Subscriber subscriber) throws ValidationException {
		Objects.requireNonNull(subscriber, "subscriber must not be null");
		if (subscriber.getId() == null)
			throw new IllegalArgumentException("subscriber ID must be set");

		@SuppressWarnings("rawtypes")
		Messenger oldMessenger = null;

		this.lock.lock();
		try {
			Subscriber current = null;
			for (Subscriber s : this.subscribers) {
				if (s.equals(subscriber)) {
					current = s;
					break;
				}
			}
			if (current == null)
				throw new IllegalArgumentException("subscriber with ID " + subscriber.getId() + " was not found");

			if (isLocked(current))
				throw new ValidationException("Could not remove an used subscriber");

			oldMessenger = removeMessenger(current);

			this.subscribers.remove(current);
			this.uses.remove(current);
			this.locks.remove(current);
		} finally {
			this.lock.unlock();
		}

		if (oldMessenger != null) {
			oldMessenger.dispose();
			cancelTasksFor(subscriber);
		}
	}

	/**
	 * Get a subscriber by ID
	 * 
	 * @param id
	 *            the ID of the subscriber
	 * @return the subscriber or null if no subscriber was found by the
	 *         specified ID
	 */
	@Override
	public Subscriber get(String id) {
		Objects.requireNonNull(id, "ID must not be null");
		this.lock.lock();
		try {
			for (Subscriber s : this.subscribers) {
				if (id.equals(s.getId())) {
					return s;
				}
			}
		} finally {
			this.lock.unlock();
		}
		return null;
	}

	/**
	 * @return the reference to the list of subscribers
	 */
	@Override
	public Collection<Subscriber> getSubscribers() {
		return this.subscribers;
	}

	/**
	 * @return true if at least one subscriber is enabled, false otherwise.
	 */
	@Override
	public boolean hasEnabledSubscribers() {
		return this.hasEnabledSubscribers;
	}

	/**
	 * @return true if at least one listener is registered, false otherwise
	 */
	@Override
	public boolean hasListeners() {
		return this.hasListeners;
	}

	/**
	 * Asynchronously send a message to all enabled subscribers and listeners
	 * 
	 * @param message
	 *            the message to send
	 * @return map of {@link Subscription} and {@link FutureSendTask} to wait
	 *         for sending to finish and handle exceptions of each send request.
	 *         You can safely ignore the result, exceptions will be logged by
	 *         default.
	 */
	@Override
	public Map<Subscription, FutureSendTask> send(Object message) {
		return send(message, false);
	}

	/**
	 * Asynchronously send a message to all enabled subscribers and listeners or
	 * only the listeners
	 * 
	 * @param message
	 *            the message to send
	 * @param listenersOnly
	 *            whether to send to listeners only
	 * @return map of {@link Subscription} and {@link FutureSendTask} to wait
	 *         for sending to finish and handle exceptions of each send request.
	 *         You can safely ignore the result, exceptions will be logged.
	 */
	@Override
	public Map<Subscription, FutureSendTask> send(Object message, boolean listenersOnly) {
		return send(message, null, listenersOnly);
	}

	/**
	 * Asynchronously send a message to all enabled subscribers and listeners,
	 * which are accepted by the specified filter
	 * 
	 * @param message
	 *            the message to send
	 * @param filter
	 *            the filter to use to accept subscribers and listeners
	 * 
	 * @return map of {@link Subscription} and {@link FutureSendTask} to wait
	 *         for sending to finish and handle exceptions of each send request.
	 *         You can safely ignore the result, exceptions will be logged.
	 */
	@Override
	public Map<Subscription, FutureSendTask> send(Object message, SubscriberFilter filter) {
		return send(message, filter, false);
	}

	protected Map<Subscription, FutureSendTask> send(Object message, SubscriberFilter filter, boolean listenersOnly) {
		Map<Subscription, FutureSendTask> result = new LinkedHashMap<>();
		@SuppressWarnings("rawtypes")
		Map<Subscription, CommonMessenger> copy;
		this.lock.lock();
		try {
			copy = getMessengersSnapshot();
			clearListenerMessengers();
		} finally {
			this.lock.unlock();
		}
		for (@SuppressWarnings("rawtypes")
		Entry<Subscription, CommonMessenger> entry : copy.entrySet()) {
			if ((filter != null && filter.accept(entry.getKey())) || (filter == null && (!listenersOnly || entry.getKey() instanceof SubscriberListener)))
				result.put(entry.getKey(), addTask(entry.getKey(), send(entry.getValue(), message)));
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	protected FutureSendTask send(@SuppressWarnings("rawtypes") Messenger messenger, Object message) {
		return messenger.send(message);
	}

	/**
	 * @return the available subscriber types or URI schemes
	 */
	@Override
	public List<String> getSubscriberTypes() {
		return Utils.getAvailableTransporters();
	}

	/**
	 * Dispose all active messengers and cancel listeners which have not
	 * received a message yet
	 */
	@Override
	public void dispose() {
		dispose(true);
	}

	/**
	 * Dispose all active messengers
	 * 
	 * @param cancelListeners
	 *            whether to also cancel listeners which have not received a
	 *            message yet
	 */
	@Override
	public void dispose(boolean cancelListeners) {
		@SuppressWarnings("rawtypes")
		Map<Subscription, CommonMessenger> copy;
		this.lock.lock();
		try {
			copy = getMessengersSnapshot();
		} finally {
			this.lock.unlock();
		}

		int pass = 0;
		while (pass <= 1) {
			pass++;
			@SuppressWarnings("rawtypes")
			Iterator<Entry<Subscription, CommonMessenger>> it = copy.entrySet().iterator();
			while (it.hasNext()) {
				@SuppressWarnings("rawtypes")
				Entry<Subscription, CommonMessenger> messenger = it.next();
				// If cancellation of listeners is requested, call dispose on
				// the listener messenger to cancel. For regular messengers,
				// first dispose all messengers in error state, then all the
				// others.
				boolean isListener = messenger.getKey() instanceof SubscriberListener;
				if ((isListener && cancelListeners) || (!isListener && (pass == 1 && messenger.getValue().isErrorState() || pass > 1))) {
					messenger.getValue().dispose();
					it.remove();
				}
			}
		}

		this.lock.lock();
		try {
			clearMessengersExcept(copy);
			disposeTasks();
		} finally {
			this.lock.unlock();
		}
	}
}