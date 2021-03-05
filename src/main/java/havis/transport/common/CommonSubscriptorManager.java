package havis.transport.common;

import havis.transport.Callback;
import havis.transport.FutureSendTask;
import havis.transport.Messenger;
import havis.transport.Subscriber;
import havis.transport.Subscriptor;
import havis.transport.SubscriptorManager;
import havis.transport.TransportException;
import havis.transport.ValidationException;

import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CommonSubscriptorManager extends TaskHandler implements SubscriptorManager {

	protected final static Logger log = Logger.getLogger(CommonSubscriptorManager.class.getName());

	protected Lock lock = new ReentrantLock();
	protected CommonSubscriberManager manager;
	protected Map<Subscriptor, Subscriber> subscriptors = new LinkedHashMap<>();
	protected boolean hasEnabledSubscriptors;
	protected Callback callback;

	public CommonSubscriptorManager(CommonSubscriberManager manager, Callback callback) {
		this(manager, callback, null);
	}

	public CommonSubscriptorManager(CommonSubscriberManager manager, Callback callback, List<Subscriptor> subscriptors) {
		Objects.requireNonNull(manager, "Manager must be set");
		this.manager = manager;
		this.callback = callback;
		Set<String> ids = new HashSet<>();
		if (subscriptors != null) {
			for (Subscriptor s : subscriptors) {
				Subscriber subscriber = manager.get(s.getSubscriberId());
				try {
					if (subscriber == null)
						throw new IllegalArgumentException("Unknown subscriber ID");
					if (s.getId() == null)
						throw new IllegalArgumentException("subscriber ID must be set");
					if (ids.contains(s.getId()))
						throw new IllegalArgumentException("subscriber ID must be unique");
					ids.add(s.getId());
					manager.lock(subscriber);
					boolean success = false;
					try {
						if (s.isEnable()) {
							if (subscriber.isEnable()) {
								manager.use(subscriber);
								boolean addedPath = false;
								try {
									addPath(s, subscriber);
									addedPath = true;
								} finally {
									if (!addedPath)
										manager.unuse(subscriber);
								}
							} else {
								throw new IllegalArgumentException("Failed to enable subscriptor using a disabled subscriber");
							}
						}
						this.subscriptors.put(s, subscriber);
						this.hasEnabledSubscriptors = this.hasEnabledSubscriptors || s.isEnable();
						success = true;
					} finally {
						if (!success)
							manager.unlock(subscriber);
					}
				} catch (Exception e) {
					log.log(Level.FINE, "Failed to load subscriptor", e);
				}

			}
		}
	}

	public CommonSubscriptorManager(CommonSubscriberManager manager) {
		this(manager, null, null);
	}

	private String getPath(URI uri, String path) {
		if (path == null || path.trim().length() == 0) {
			return null;
		}
		if (uri.getPath() == null || uri.getPath().length() == 0)
			return path.startsWith("/") ? path : "/" + path;
		else
			return uri.getPath().endsWith("/") ? uri.getPath() + path : uri.getPath() + "/" + path;
	}

	private void addPath(Subscriptor subscriptor, Subscriber subscriber) throws TransportException {
		if (callback != null) {
			@SuppressWarnings("rawtypes")
			CommonMessenger messenger = manager.getMessenger(subscriber);
			messenger.addPath(getPath(messenger.getUri(), subscriptor.getPath()), callback);
		}
	}

	private void removePath(Subscriptor subscriptor, Subscriber subscriber) throws TransportException {
		if (callback != null) {
			@SuppressWarnings("rawtypes")
			CommonMessenger messenger = manager.getMessenger(subscriber);
			messenger.removePath(getPath(messenger.getUri(), subscriptor.getPath()), callback);
		}
	}

	@Override
	public void update(Subscriptor subscriptor) throws TransportException, ValidationException {
		// damage control
		Objects.requireNonNull(subscriptor, "Subscriptor must be set");
		if (subscriptor.getId() == null)
			throw new ValidationException("Subscriptor ID must be set");
		Subscriptor current = null;
		Subscriber subscriber = null;
		Subscriber newSubscriber = null;
		lock.lock();
		try {
			current = get(subscriptor);
			if (current == null)
				throw new ValidationException("Unknown subscriptor " + subscriptor.getId());
			if (subscriptor.getSubscriberId() == null)
				throw new ValidationException("Subscriber ID must be set");
			subscriber = subscriptors.get(subscriptor);
			if (!current.getSubscriberId().equals(subscriptor.getSubscriberId())) {
				newSubscriber = manager.get(subscriptor.getSubscriberId());

				if (newSubscriber == null)
					throw new ValidationException("Unknown subscriber ID");
			}
			if (subscriptor.isEnable()) {
				if (newSubscriber == null) {
					if (!current.isEnable() && !subscriber.isEnable()) {
						throw new TransportException("Could not enable a subscriptor while subscriber is disabled");
					}
				} else {
					if (!newSubscriber.isEnable()) {
						throw new TransportException("Could not enable a subscriptor while subscriber is disabled");
					}
				}
			}
		} finally {
			lock.unlock();
		}
		// end damage control

		if (current.isEnable()) {
			if (!subscriptor.isEnable()) {
				try {
					removePath(current, subscriber);
				} catch (TransportException e) {
					log.log(Level.FINE, "Failed to remove path from subscriber", e);
				}
				if (newSubscriber != null) {
					manager.unlock(subscriber);
					manager.lock(newSubscriber);
				}
				manager.unuse(subscriber);
				cancelTasksFor(current);
			} else {
				if (newSubscriber != null) {
					addPath(subscriptor, newSubscriber);
					manager.lock(newSubscriber);
					manager.use(newSubscriber);
					try {
						removePath(current, subscriber);
					} catch (TransportException e) {
						log.log(Level.FINE, "Failed to remove path from subscriber", e);
					}
					manager.unlock(subscriber);
					manager.unuse(subscriber);
				} else {
					if (current.getPath() != subscriptor.getPath() && (current.getPath() == null || !current.getPath().equals(subscriptor.getPath()))) {
						addPath(subscriptor, subscriber);// new path
						try {
							removePath(current, subscriber);
						} catch (TransportException e) {
							log.log(Level.FINE, "Failed to remove path from subscriber", e);
						}
					}
				}

			}
		} else {
			if (subscriptor.isEnable()) {
				if (newSubscriber != null) {
					addPath(subscriptor, newSubscriber);
					manager.unlock(subscriber);
					manager.lock(newSubscriber);
					manager.use(newSubscriber);
				} else {

					addPath(subscriptor, subscriber);
					manager.use(subscriber);
				}
			} else {
				if (newSubscriber != null) {
					manager.unlock(subscriber);
					manager.lock(newSubscriber);
				}
			}
		}
		lock.lock();
		try {
			subscriptors.remove(current);// same thing but better understanding
			if (newSubscriber != null) {
				subscriptors.put(subscriptor, newSubscriber);
			} else {
				subscriptors.put(subscriptor, subscriber);
			}
			this.hasEnabledSubscriptors = recalculateHasEnabled();
		} finally {
			lock.unlock();
		}
	}

	void init(Map<String, String> properties) throws ValidationException {
		// TODO: validaton
		if (properties != null) {
			for (Entry<String, String> entry : properties.entrySet()) {
				if (entry.getKey() != null) {
					switch (entry.getKey()) {
					case Messenger.MIMETYPE_PROPERTY:
						throw new ValidationException("Cannot set property " + Messenger.MIMETYPE_PROPERTY + " for subscription");
					}
				}
			}
		}
	}

	/**
	 * Adds a subscriptor, ID will be generated
	 * 
	 * @param subscriptor
	 *            the subscriber without and ID (will be generated)
	 * @return the generated ID
	 * @throws ValidationException
	 *             if validation of subscriber failed
	 */
	@Override
	public String add(Subscriptor subscriptor) throws ValidationException, TransportException {
		Objects.requireNonNull(subscriptor, "Subscriptor must be set");
		if (subscriptor.getId() != null)
			throw new ValidationException("Subscriptor ID must not be set");
		if (subscriptor.getSubscriberId() == null)
			throw new ValidationException("Subscriber ID must be set");
		Subscriber subscriber = manager.get(subscriptor.getSubscriberId());

		if (subscriber == null)
			throw new ValidationException("Unknown subscriber ID");
		manager.lock(subscriber);
		subscriptor.setId(UUID.randomUUID().toString());
		if (subscriptor.isEnable()) {
			if (subscriber.isEnable()) {
				manager.use(subscriber);
				try {
					addPath(subscriptor, subscriber);// this can crash
				} catch (TransportException ex) {
					manager.unuse(subscriber);
					manager.unlock(subscriber);
					throw ex;
				}

			} else {
				subscriptor.setId(null);
				throw new ValidationException("Could not enable a subscriptor on a disabled subscriber");
			}
		}
		this.lock.lock();
		try {
			this.subscriptors.put(subscriptor, subscriber);
			this.hasEnabledSubscriptors = this.hasEnabledSubscriptors || subscriptor.isEnable();
		} finally {
			this.lock.unlock();
		}
		return subscriptor.getId();
	}

	@Override
	public void remove(String id) throws ValidationException, TransportException {
		Subscriptor subscriptor = new Subscriptor();
		subscriptor.setId(id);
		remove(subscriptor);
	}

	private Subscriptor get(Subscriptor subscriptor) {
		for (Subscriptor current : subscriptors.keySet())
			if (current.equals(subscriptor))
				return current;
		return null;
	}

	@Override
	public void remove(Subscriptor subscriptor) throws ValidationException, TransportException {
		Objects.requireNonNull(subscriptor, "Subscriptor must be set");
		if (subscriptor.getId() == null)
			throw new ValidationException("Subscriptor ID must be set");

		this.lock.lock();
		try {
			Subscriptor current = get(subscriptor);
			if (current == null)
				throw new ValidationException("Unknown subscriptor " + subscriptor.getId());
			Subscriber subscriber = subscriptors.remove(subscriptor);
			if (current.isEnable()) {
				manager.unuse(subscriber);
				removePath(current, subscriber);
				cancelTasksFor(subscriptor);
			}
			manager.unlock(subscriber);
			this.hasEnabledSubscriptors = recalculateHasEnabled();
		} finally {
			this.lock.unlock();
		}
	}

	private boolean recalculateHasEnabled() {
		for (Subscriptor s : subscriptors.keySet())
			if (s.isEnable())
				return true;
		return false;
	}

	@Override
	public Collection<Subscriptor> getSubscriptors() {
		return this.subscriptors.keySet();
	}

	@Override
	@SuppressWarnings("unchecked")
	public Map<Subscriptor, FutureSendTask> send(Object message) {
		Map<Subscriptor, FutureSendTask> result = new LinkedHashMap<>();
		Map<Subscriptor, Subscriber> copy;
		this.lock.lock();
		try {
			copy = new LinkedHashMap<>(this.subscriptors);
		} finally {
			this.lock.unlock();
		}
		for (Entry<Subscriptor, Subscriber> entry : copy.entrySet()) {
			Subscriptor subscriptor = entry.getKey();
			if (subscriptor.isEnable()) {
				Subscriber subscriber = entry.getValue();
				@SuppressWarnings("rawtypes")
				CommonMessenger messenger = manager.getMessenger(subscriber);
				FutureSendTask task = messenger.send(message, subscriptor.getName(), getPath(messenger.getUri(), subscriptor.getPath()),
						subscriptor.getProperties());
				result.put(subscriptor, addTask(subscriptor, task));
			}
		}
		return result;
	}

	@Override
	public void dispose() {
		this.lock.lock();
		try {
			for (Entry<Subscriptor, Subscriber> entry : new LinkedHashMap<>(subscriptors).entrySet()) {
				try {
					remove(entry.getKey());
				} catch (Exception e) {
					log.log(Level.FINE, "Failed to dispose subscriptor", e);
				}
			}
			this.subscriptors.clear();
			this.hasEnabledSubscriptors = false;
			disposeTasks();
		} finally {
			this.lock.unlock();
		}
	}

	@Override
	public boolean hasEnabledSubscriptors() {
		return this.hasEnabledSubscriptors;
	}
}