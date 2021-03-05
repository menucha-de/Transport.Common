package havis.transport.common;

import havis.transport.FutureSendTask;
import havis.transport.MessageReceiver;
import havis.transport.Messenger;
import havis.transport.Subscriber;
import havis.transport.SubscriberFilter;
import havis.transport.SubscriberListener;
import havis.transport.SubscriberManager;
import havis.transport.Subscription;
import havis.transport.Transporter;
import havis.transport.ValidationException;
import havis.util.monitor.Broker;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.MessageBodyWriter;

import mockit.Deencapsulation;
import mockit.Mocked;
import mockit.NonStrictExpectations;
import mockit.Verifications;
import mockit.VerificationsInOrder;

import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class CommonSubscriberManagerTest {

	@Test
	public void init(final @Mocked CommonMessenger messenger) throws Exception {
		try {
			new CommonSubscriberManager(Person.class, Arrays.asList(new Subscriber(false, "http://whatever")));
			Assert.fail("Expected IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			// OK
		}

		try {
			Subscriber notUnique1 = new Subscriber(false, "http://whatever");
			notUnique1.setId("notUnique");
			Subscriber notUnique2 = new Subscriber(false, "http://whatever");
			notUnique2.setId("notUnique");
			new CommonSubscriberManager(Person.class, Arrays.asList(notUnique1, notUnique2));
			Assert.fail("Expected IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			// OK
		}

		List<Subscriber> subscribers = new ArrayList<>();
		final Map<String, String> properties1 = new HashMap<>();
		Subscriber s1 = new Subscriber(true, "http://test1", properties1);
		s1.setId("1");
		subscribers.add(s1);
		final Map<String, String> properties2 = new HashMap<>();
		Subscriber s2 = new Subscriber(false, "http://test2", properties2);
		s2.setId("2");
		subscribers.add(s2);
		final Map<String, String> properties3 = new HashMap<>();
		Subscriber s3 = new Subscriber(true, "http://test3", properties3);
		s3.setId("3");
		subscribers.add(s3);
		SubscriberManager manager = new CommonSubscriberManager(Person.class, subscribers);

		Assert.assertTrue(manager.hasEnabledSubscribers());

		Assert.assertEquals(3, subscribers.size());
		Assert.assertSame(s1, subscribers.get(0));
		Assert.assertSame(s2, subscribers.get(1));
		Assert.assertSame(s3, subscribers.get(2));

		Map<Subscriber, Messenger> messengers = Deencapsulation.getField(manager, "messengers");
		Assert.assertEquals(2, messengers.size());

		new VerificationsInOrder() {
			{
				messenger.init(withSameInstance(Person.class), withEqual(new URI("http://test1")), withSameInstance(properties1));
				times = 1;

				messenger.init(withSameInstance(Person.class), withEqual(new URI("http://test3")), withSameInstance(properties3));
				times = 1;
			}
		};
	}

	@Test
	public void initDefault(final @Mocked CommonMessenger messenger) throws Exception {
		List<Subscriber> subscribers = new ArrayList<>();
		final Map<String, String> properties1 = new HashMap<>();
		properties1.put("test1", "value1");
		Subscriber s1 = new Subscriber(true, "http://test1", properties1);
		s1.setId("1");
		subscribers.add(s1);
		final Map<String, String> properties2 = new HashMap<>();
		properties2.put("test2", "value2");
		properties2.put(Messenger.MIMETYPE_PROPERTY, "application/json");
		Subscriber s2 = new Subscriber(true, "http://test2", properties2);
		s2.setId("2");
		subscribers.add(s2);

		Map<String, String> defaultProperties = new HashMap<>();
		defaultProperties.put(Messenger.MIMETYPE_PROPERTY, "text/xml");
		SubscriberManager manager = new CommonSubscriberManager(Person.class, subscribers, defaultProperties);

		Assert.assertTrue(manager.hasEnabledSubscribers());

		Assert.assertEquals(2, subscribers.size());
		Assert.assertSame(s1, subscribers.get(0));
		Assert.assertSame(s2, subscribers.get(1));

		Map<Subscriber, Messenger> messengers = Deencapsulation.getField(manager, "messengers");
		Assert.assertEquals(2, messengers.size());

		new VerificationsInOrder() {
			{
				Map<String, String> p;
				messenger.init(withSameInstance(Person.class), withEqual(new URI("http://test1")), p = withCapture());
				times = 1;

				Assert.assertNotSame(properties1, p);
				Assert.assertEquals(2, p.size());
				Assert.assertEquals("text/xml", p.get(Messenger.MIMETYPE_PROPERTY));
				Assert.assertEquals("value1", p.get("test1"));

				messenger.init(withSameInstance(Person.class), withEqual(new URI("http://test2")), p = withCapture());
				times = 1;

				Assert.assertNotSame(properties1, p);
				Assert.assertEquals(2, p.size());
				Assert.assertEquals("application/json", p.get(Messenger.MIMETYPE_PROPERTY));
				Assert.assertEquals("value2", p.get("test2"));
			}
		};
	}

	@Test
	public void add(final @Mocked CommonMessenger messenger) throws Exception {
		List<Subscriber> subscribers = new ArrayList<>();
		SubscriberManager manager = new CommonSubscriberManager(Person.class, subscribers);

		Assert.assertFalse(manager.hasEnabledSubscribers());

		try {
			manager.add((Subscriber) null);
			Assert.fail("Expected NullPointerException");
		} catch (NullPointerException e) {
			// OK
		}

		try {
			Subscriber subscriberWithId = new Subscriber();
			subscriberWithId.setId("whatever");
			manager.add(subscriberWithId);
			Assert.fail("Expected IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			// OK
		}

		try {
			Subscriber subscriberWithBadUri = new Subscriber(true, ":::");
			manager.add(subscriberWithBadUri);
			Assert.fail("Expected ValidationException");
		} catch (ValidationException e) {
			// OK
		}

		final Map<String, String> properties = new HashMap<String, String>();
		Subscriber subscriber = new Subscriber(true, "http://test", properties);
		Assert.assertNull(subscriber.getId());
		manager.add(subscriber);
		Assert.assertNotNull(subscriber.getId());
		Assert.assertEquals(1, subscribers.size());
		Assert.assertSame(subscriber, subscribers.get(0));

		Assert.assertTrue(manager.hasEnabledSubscribers());

		Map<Subscriber, Messenger> messengers = Deencapsulation.getField(manager, "messengers");
		Assert.assertEquals(1, messengers.size());

		new Verifications() {
			{
				messenger.init(withSameInstance(Person.class), withEqual(new URI("http://test")), withSameInstance(properties));
				times = 1;
			}
		};
	}

	@Test
	public void addDisabled(final @Mocked CommonMessenger messenger) throws Exception {
		List<Subscriber> subscribers = new ArrayList<>();
		SubscriberManager manager = new CommonSubscriberManager(Person.class, subscribers);

		Assert.assertFalse(manager.hasEnabledSubscribers());

		final Map<String, String> properties = new HashMap<String, String>();
		Subscriber subscriber = new Subscriber(false, "http://test", properties);
		Assert.assertNull(subscriber.getId());
		manager.add(subscriber);
		Assert.assertNotNull(subscriber.getId());
		Assert.assertEquals(1, subscribers.size());
		Assert.assertSame(subscriber, subscribers.get(0));

		Assert.assertFalse(manager.hasEnabledSubscribers());

		Map<Subscriber, Messenger> messengers = Deencapsulation.getField(manager, "messengers");
		Assert.assertEquals(0, messengers.size());

		new Verifications() {
			{
				messenger.init(withSameInstance(Person.class), withEqual(new URI("http://test")), withSameInstance(properties));
				times = 0;
			}
		};
	}

	@Test
	public void update(final @Mocked CommonMessenger messenger) throws Exception {
		List<Subscriber> subscribers = new ArrayList<>();
		SubscriberManager manager = new CommonSubscriberManager(Person.class, subscribers);

		Assert.assertFalse(manager.hasEnabledSubscribers());

		try {
			manager.update(null);
			Assert.fail("Expected NullPointerException");
		} catch (NullPointerException e) {
			// OK
		}

		try {
			manager.update(new Subscriber());
			Assert.fail("Expected IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			// OK
		}

		try {
			Subscriber subscriberNotFound = new Subscriber();
			subscriberNotFound.setId("whatever");
			manager.update(subscriberNotFound);
			Assert.fail("Expected IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			// OK
		}

		final Map<String, String> properties1 = new HashMap<String, String>();
		properties1.put("whatever", "whatever");
		Subscriber subscriber = new Subscriber(true, "http://test1", properties1);
		manager.add(subscriber);
		Assert.assertEquals(1, subscribers.size());
		Assert.assertSame(subscriber, subscribers.get(0));

		Assert.assertTrue(manager.hasEnabledSubscribers());

		Map<Subscriber, Messenger> messengers = Deencapsulation.getField(manager, "messengers");
		Assert.assertEquals(1, messengers.size());

		final Map<String, String> properties2 = new HashMap<String, String>();
		properties2.put("whatever", "whatever");
		Subscriber update = new Subscriber(true, "http://test2", properties2);
		update.setId(subscriber.getId());
		manager.update(update);
		Assert.assertEquals(1, subscribers.size());
		// to keep the order, it will be the old subscriber
		Assert.assertSame(subscriber, subscribers.get(0));
		Assert.assertEquals(update.getUri(), subscriber.getUri());
		Assert.assertNotSame(update.getProperties(), subscriber.getProperties());
		Assert.assertEquals(update.getProperties(), subscriber.getProperties());

		Assert.assertTrue(manager.hasEnabledSubscribers());

		Assert.assertEquals(1, messengers.size());

		new VerificationsInOrder() {
			{
				messenger.init(withSameInstance(Person.class), withEqual(new URI("http://test1")), withSameInstance(properties1));
				times = 1;

				messenger.init(withSameInstance(Person.class), withEqual(new URI("http://test2")), withSameInstance(properties2));
				times = 1;

				messenger.dispose();
				times = 1;
			}
		};
	}

	/**
	 * messenger is initialized, but then disposed because it was not found
	 */
	@Test
	public void updateDisposeNotFound(final @Mocked CommonMessenger messenger) throws Exception {
		List<Subscriber> subscribers = new ArrayList<>();
		SubscriberManager manager = new CommonSubscriberManager(Person.class, subscribers);

		try {
			Subscriber update = new Subscriber(true, "http://test", new HashMap<String, String>());
			update.setId("whatever");
			manager.update(update);
			Assert.fail("Expected IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			// OK
		}

		new Verifications() {
			{
				messenger.init(withSameInstance(Person.class), this.<URI> withNotNull(), this.<Map> withNotNull());
				times = 1;

				messenger.dispose();
				times = 1;
			}
		};
	}

	@Test
	public void updateEnable(final @Mocked CommonMessenger messenger) throws Exception {
		List<Subscriber> subscribers = new ArrayList<>();
		SubscriberManager manager = new CommonSubscriberManager(Person.class, subscribers);

		Assert.assertFalse(manager.hasEnabledSubscribers());

		Subscriber subscriber = new Subscriber(false, "http://test", new HashMap<String, String>());
		manager.add(subscriber);
		Assert.assertEquals(1, subscribers.size());
		Assert.assertSame(subscriber, subscribers.get(0));

		Assert.assertFalse(manager.hasEnabledSubscribers());

		Map<Subscriber, Messenger> messengers = Deencapsulation.getField(manager, "messengers");
		Assert.assertEquals(0, messengers.size());

		Subscriber update = new Subscriber(true, "http://test", new HashMap<String, String>());
		update.setId(subscriber.getId());
		manager.update(update);
		Assert.assertEquals(1, subscribers.size());
		// to keep the order, it will be the old subscriber
		Assert.assertSame(subscriber, subscribers.get(0));
		Assert.assertTrue(subscriber.isEnable());

		Assert.assertTrue(manager.hasEnabledSubscribers());

		Assert.assertEquals(1, messengers.size());

		new Verifications() {
			{
				messenger.init(withSameInstance(Person.class), this.<URI> withNotNull(), this.<Map> withNotNull());
				times = 1;

				messenger.dispose();
				times = 0;
			}
		};
	}

	@Test
	public void updateDisable(final @Mocked CommonMessenger messenger) throws Exception {
		List<Subscriber> subscribers = new ArrayList<>();
		SubscriberManager manager = new CommonSubscriberManager(Person.class, subscribers);

		Assert.assertFalse(manager.hasEnabledSubscribers());

		Subscriber subscriber = new Subscriber(true, "http://test", new HashMap<String, String>());
		manager.add(subscriber);
		Assert.assertEquals(1, subscribers.size());
		Assert.assertSame(subscriber, subscribers.get(0));

		Assert.assertTrue(manager.hasEnabledSubscribers());

		Map<Subscriber, Messenger> messengers = Deencapsulation.getField(manager, "messengers");
		Assert.assertEquals(1, messengers.size());

		Subscriber update = new Subscriber(false, "http://test", new HashMap<String, String>());
		update.setId(subscriber.getId());
		manager.update(update);
		Assert.assertEquals(1, subscribers.size());
		// to keep the order, it will be the old subscriber
		Assert.assertSame(subscriber, subscribers.get(0));
		Assert.assertFalse(subscriber.isEnable());

		Assert.assertFalse(manager.hasEnabledSubscribers());

		Assert.assertEquals(0, messengers.size());

		new Verifications() {
			{
				messenger.init(withSameInstance(Person.class), this.<URI> withNotNull(), this.<Map> withNotNull());
				times = 1;

				messenger.dispose();
				times = 1;
			}
		};
	}

	@Test
	public void remove(final @Mocked CommonMessenger messenger) throws Exception {
		List<Subscriber> subscribers = new ArrayList<>();
		SubscriberManager manager = new CommonSubscriberManager(Person.class, subscribers);

		Assert.assertFalse(manager.hasEnabledSubscribers());

		try {
			manager.remove((Subscriber) null);
			Assert.fail("Expected NullPointerException");
		} catch (NullPointerException e) {
			// OK
		}

		try {
			manager.remove((String) null);
			Assert.fail("Expected NullPointerException");
		} catch (NullPointerException e) {
			// OK
		}

		try {
			manager.remove(new Subscriber());
			Assert.fail("Expected IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			// OK
		}

		try {
			manager.remove("whatever");
			Assert.fail("Expected IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			// OK
		}

		try {
			Subscriber subscriberWithBadUri = new Subscriber(true, ":::");
			manager.add(subscriberWithBadUri);
			Assert.fail("Expected ValidationException");
		} catch (ValidationException e) {
			// OK
		}

		Subscriber subscriber = new Subscriber(true, "http://test", new HashMap<String, String>());
		manager.add(subscriber);
		Assert.assertEquals(1, subscribers.size());
		Assert.assertSame(subscriber, subscribers.get(0));

		Assert.assertTrue(manager.hasEnabledSubscribers());

		Map<Subscriber, Messenger> messengers = Deencapsulation.getField(manager, "messengers");
		Assert.assertEquals(1, messengers.size());

		Subscriber remove = new Subscriber();
		remove.setId(subscriber.getId());
		manager.remove(remove);
		Assert.assertEquals(0, subscribers.size());

		Assert.assertFalse(manager.hasEnabledSubscribers());

		Assert.assertEquals(0, messengers.size());

		new Verifications() {
			{
				messenger.init(withSameInstance(Person.class), this.<URI> withNotNull(), this.<Map> withNotNull());
				times = 1;

				messenger.dispose();
				times = 1;
			}
		};
	}

	@Test
	public void removeDisabled(final @Mocked CommonMessenger messenger) throws Exception {
		List<Subscriber> subscribers = new ArrayList<>();
		SubscriberManager manager = new CommonSubscriberManager(Person.class, subscribers);

		Assert.assertFalse(manager.hasEnabledSubscribers());

		Subscriber subscriber = new Subscriber(false, "http://test", new HashMap<String, String>());
		manager.add(subscriber);
		Assert.assertEquals(1, subscribers.size());
		Assert.assertSame(subscriber, subscribers.get(0));

		Assert.assertFalse(manager.hasEnabledSubscribers());

		Map<Subscriber, Messenger> messengers = Deencapsulation.getField(manager, "messengers");
		Assert.assertEquals(0, messengers.size());

		Subscriber remove = new Subscriber();
		remove.setId(subscriber.getId());
		manager.remove(remove);
		Assert.assertEquals(0, subscribers.size());

		Assert.assertFalse(manager.hasEnabledSubscribers());

		Assert.assertEquals(0, messengers.size());

		new Verifications() {
			{
				messenger.init(withSameInstance(Person.class), this.<URI> withNotNull(), this.<Map> withNotNull());
				times = 0;

				messenger.dispose();
				times = 0;
			}
		};
	}

	@Test
	public void get(final @Mocked CommonMessenger messenger) throws Exception {
		List<Subscriber> subscribers = new ArrayList<>();
		SubscriberManager manager = new CommonSubscriberManager(Person.class, subscribers);

		try {
			manager.get(null);
			Assert.fail("Expected NullPointerException");
		} catch (NullPointerException e) {
			// OK
		}

		Assert.assertNull(manager.get("notfound"));

		Subscriber subscriber = new Subscriber(true, "http://test", new HashMap<String, String>());
		manager.add(subscriber);
		Assert.assertNotNull(subscriber.getId());
		Assert.assertEquals(1, subscribers.size());
		Assert.assertSame(subscriber, subscribers.get(0));
		Assert.assertSame(subscriber, manager.get(subscriber.getId()));
	}

	@Test
	public void getSubscribers(final @Mocked CommonMessenger messenger) throws Exception {
		List<Subscriber> subscribers = new ArrayList<>();
		final Map<String, String> properties1 = new HashMap<>();
		Subscriber s1 = new Subscriber(false, "http://test1", properties1);
		s1.setId("1");
		subscribers.add(s1);
		final Map<String, String> properties2 = new HashMap<>();
		Subscriber s2 = new Subscriber(false, "http://test2", properties2);
		s2.setId("2");
		subscribers.add(s2);
		final Map<String, String> properties3 = new HashMap<>();
		Subscriber s3 = new Subscriber(false, "http://test3", properties3);
		s3.setId("3");
		subscribers.add(s3);
		SubscriberManager manager = new CommonSubscriberManager(Person.class, subscribers);

		Assert.assertSame(subscribers, manager.getSubscribers());
	}

	@Test
	public void send(final @Mocked CommonMessenger messenger) throws Exception {
		List<Subscriber> subscribers = new ArrayList<>();
		final Map<String, String> properties1 = new HashMap<>();
		Subscriber s1 = new Subscriber(true, "http://test1", properties1);
		s1.setId("1");
		subscribers.add(s1);
		final Map<String, String> properties2 = new HashMap<>();
		Subscriber s2 = new Subscriber(false, "http://test2", properties2);
		s2.setId("2");
		subscribers.add(s2);
		final Map<String, String> properties3 = new HashMap<>();
		Subscriber s3 = new Subscriber(true, "http://test3", properties3);
		s3.setId("3");
		subscribers.add(s3);
		SubscriberManager manager = new CommonSubscriberManager(Person.class, subscribers);

		Assert.assertTrue(manager.hasEnabledSubscribers());

		final Person person = new Person("test");
		manager.send(person);

		new VerificationsInOrder() {
			{
				messenger.init(withSameInstance(Person.class), withEqual(new URI("http://test1")), withSameInstance(properties1));
				times = 1;

				messenger.init(withSameInstance(Person.class), withEqual(new URI("http://test3")), withSameInstance(properties3));
				times = 1;

				messenger.send(withSameInstance(person));
				times = 2;
			}
		};
	}

	@Test
	public void sendWithFilter(final @Mocked CommonMessenger messenger) throws Exception {
		List<Subscriber> subscribers = new ArrayList<>();
		final Map<String, String> properties1 = new HashMap<>();
		Subscriber s1 = new Subscriber(true, "http://test1", properties1);
		s1.setId("1");
		subscribers.add(s1);
		final Map<String, String> properties2 = new HashMap<>();
		Subscriber s2 = new Subscriber(true, "http://test2", properties2);
		s2.setId("2");
		subscribers.add(s2);
		final Map<String, String> properties3 = new HashMap<>();
		Subscriber s3 = new Subscriber(true, "http://test3", properties3);
		s3.setId("3");
		subscribers.add(s3);
		SubscriberManager manager = new CommonSubscriberManager(Person.class, subscribers);

		Assert.assertTrue(manager.hasEnabledSubscribers());

		final Person person = new Person("test");
		manager.send(person, new SubscriberFilter() {
			@Override
			public boolean accept(Subscription subscription) {
				return "2".equals(subscription.getId());
			}
		});

		new VerificationsInOrder() {
			{
				messenger.init(withSameInstance(Person.class), withEqual(new URI("http://test1")), withSameInstance(properties1));
				times = 1;
				
				messenger.init(withSameInstance(Person.class), withEqual(new URI("http://test2")), withSameInstance(properties2));
				times = 1;

				messenger.init(withSameInstance(Person.class), withEqual(new URI("http://test3")), withSameInstance(properties3));
				times = 1;

				messenger.send(withSameInstance(person));
				times = 1;
			}
		};
	}

	@Test
	public void sendWithListener(final @Mocked CommonMessenger messenger) throws Exception {
		List<Subscriber> subscribers = new ArrayList<>();
		final Map<String, String> properties1 = new HashMap<>();
		Subscriber s1 = new Subscriber(true, "http://test1", properties1);
		s1.setId("1");
		subscribers.add(s1);
		SubscriberManager manager = new CommonSubscriberManager(Person.class, subscribers);

		Assert.assertTrue(manager.hasEnabledSubscribers());
		Assert.assertFalse(manager.hasListeners());

		final Map<String, String> properties2 = new HashMap<>();
		final SubscriberListener listener = new SubscriberListener(new MessageReceiver() {
			@Override
			public void receive(Object message) {
			}

			@Override
			public void cancel() {
			}

		}, properties2);
		manager.add(listener);

		Assert.assertTrue(manager.hasListeners());

		final Person person = new Person("test");
		manager.send(person);

		Assert.assertFalse(manager.hasListeners());

		new VerificationsInOrder() {
			{
				messenger.init(withSameInstance(Person.class), withEqual(new URI("http://test1")), withSameInstance(properties1));
				times = 1;

				messenger.init(withSameInstance(Person.class), withEqual((MessageReceiver) listener), withSameInstance(properties2));
				times = 1;

				messenger.send(withSameInstance(person));
				times = 2;
			}
		};
	}

	@Test
	public void sendToListenerOnly(@Mocked final Provider provider, @Mocked final MessageBodyWriter<Object> writer) throws Exception {
		new NonStrictExpectations() {
			{
				Provider.getFactory();
				result = provider;

				provider.getMessageBodyWriter((Class) any, (Type) any, (Annotation[]) any, (MediaType) any);
				result = writer;
			}
		};

		List<Subscriber> subscribers = new ArrayList<>();
		final Map<String, String> properties1 = new HashMap<>();
		Subscriber s1 = new Subscriber(true, "http://test1", properties1);
		s1.setId("1");
		subscribers.add(s1);
		final List<AtomicBoolean> disposedMessengers = new ArrayList<>();
		SubscriberManager manager = new CommonSubscriberManager(Person.class, subscribers) {
			@Override
			CommonMessenger createMessenger() {
				return new CommonMessenger() {
					AtomicBoolean disposed = new AtomicBoolean(false);
					{
						disposedMessengers.add(disposed);
					}

					@Override
					public void dispose() {
						disposed.set(true);
						super.dispose();
					}
				};
			}
		};

		Assert.assertTrue(manager.hasEnabledSubscribers());
		Assert.assertFalse(manager.hasListeners());

		final Map<String, String> properties2 = new HashMap<>();
		final AtomicReference<Person> received = new AtomicReference(null);
		final AtomicBoolean cancelled = new AtomicBoolean(false);
		final SubscriberListener listener = new SubscriberListener(new MessageReceiver() {
			@Override
			public void receive(Object message) {
				received.set((Person) message);
			}

			@Override
			public void cancel() {
				cancelled.set(true);
			}
		}, properties2);
		manager.add(listener);

		Assert.assertTrue(manager.hasEnabledSubscribers());
		Assert.assertTrue(manager.hasListeners());

		Assert.assertEquals(2, disposedMessengers.size());
		Assert.assertFalse(disposedMessengers.get(0).get());
		Assert.assertFalse(disposedMessengers.get(1).get());

		final Person person = new Person("test");
		FutureSendTask future = manager.send(person, true).get(listener);

		Assert.assertTrue(manager.hasEnabledSubscribers());
		Assert.assertFalse(manager.hasListeners());

		future.get();

		Assert.assertEquals(person, received.get());
		Assert.assertFalse(cancelled.get());

		Assert.assertEquals(2, disposedMessengers.size());
		Assert.assertFalse(disposedMessengers.get(0).get());
		Assert.assertTrue(disposedMessengers.get(1).get());
	}

	@Test
	public void sendWithListenerOnly() throws Exception {
		List<Subscriber> subscribers = new ArrayList<>();
		final List<AtomicBoolean> disposedMessengers = new ArrayList<>();
		SubscriberManager manager = new CommonSubscriberManager(Person.class, subscribers) {
			@Override
			CommonMessenger createMessenger() {
				return new CommonMessenger() {
					AtomicBoolean disposed = new AtomicBoolean(false);
					{
						disposedMessengers.add(disposed);
					}

					@Override
					public void dispose() {
						disposed.set(true);
						super.dispose();
					}
				};
			}
		};

		Assert.assertFalse(manager.hasEnabledSubscribers());
		Assert.assertFalse(manager.hasListeners());

		final Map<String, String> properties = new HashMap<>();
		final AtomicReference<Person> received = new AtomicReference(null);
		final AtomicBoolean cancelled = new AtomicBoolean(false);
		final SubscriberListener listener = new SubscriberListener(new MessageReceiver() {
			@Override
			public void receive(Object message) {
				received.set((Person) message);
			}

			@Override
			public void cancel() {
				cancelled.set(true);
			}
		}, properties);
		manager.add(listener);

		Assert.assertTrue(manager.hasEnabledSubscribers());
		Assert.assertTrue(manager.hasListeners());

		Assert.assertEquals(1, disposedMessengers.size());
		Assert.assertFalse(disposedMessengers.get(0).get());

		final Person person = new Person("test");
		FutureSendTask future = manager.send(person).get(listener);

		Assert.assertFalse(manager.hasEnabledSubscribers());
		Assert.assertFalse(manager.hasListeners());

		future.get();

		Assert.assertEquals(person, received.get());
		Assert.assertFalse(cancelled.get());

		Assert.assertEquals(1, disposedMessengers.size());
		Assert.assertTrue(disposedMessengers.get(0).get());
	}

	@Test
	public void getSubscriberTypes(final @Mocked CommonMessenger messenger) throws Exception {
		Connector.createFactory(new Connector() {
			@Override
			public <S> S newInstance(Class<S> clazz, String type) throws ValidationException {
				return null;
			}

			@Override
			public <S> List<String> getTypes(Class<S> clazz) throws ValidationException {
				if (clazz.equals(Transporter.class))
					return Arrays.asList("a", "b", "c");
				return null;
			}

			@Override
			public Broker getBroker() {
				return null;
			}
		});
		SubscriberManager manager = new CommonSubscriberManager(Person.class, new ArrayList<Subscriber>());
		try {
			Assert.assertEquals(Arrays.asList("a", "b", "c"), manager.getSubscriberTypes());
		} finally {
			Connector.clearFactory();
		}
	}

	@Test
	public void dispose(final @Mocked CommonMessenger messenger) throws Exception {
		List<Subscriber> subscribers = new ArrayList<>();
		final Map<String, String> properties1 = new HashMap<>();
		Subscriber s1 = new Subscriber(true, "http://test1", properties1);
		s1.setId("1");
		subscribers.add(s1);
		final Map<String, String> properties2 = new HashMap<>();
		Subscriber s2 = new Subscriber(false, "http://test2", properties2);
		s2.setId("2");
		subscribers.add(s2);
		final Map<String, String> properties3 = new HashMap<>();
		Subscriber s3 = new Subscriber(true, "http://test3", properties3);
		s3.setId("3");
		subscribers.add(s3);
		SubscriberManager manager = new CommonSubscriberManager(Person.class, subscribers);

		Assert.assertTrue(manager.hasEnabledSubscribers());

		Map<Subscriber, Messenger> messengers = Deencapsulation.getField(manager, "messengers");
		Assert.assertEquals(2, messengers.size());
		manager.dispose();
		Assert.assertEquals(0, messengers.size());

		Assert.assertFalse(manager.hasEnabledSubscribers());

		new VerificationsInOrder() {
			{
				messenger.init(withSameInstance(Person.class), withEqual(new URI("http://test1")), withSameInstance(properties1));
				times = 1;

				messenger.init(withSameInstance(Person.class), withEqual(new URI("http://test3")), withSameInstance(properties3));
				times = 1;

				messenger.dispose();
				times = 2;
			}
		};
	}

	@Test
	public void disposeOrderly(final @Mocked CommonMessenger messenger1, final @Mocked CommonMessenger messenger2) throws Exception {

		new NonStrictExpectations() {
			{
				messenger1.isErrorState();
				result = Boolean.FALSE;

				messenger2.isErrorState();
				result = Boolean.TRUE;
			}
		};

		List<Subscriber> subscribers = new ArrayList<>();
		final Map<String, String> properties1 = new HashMap<>();
		Subscriber s1 = new Subscriber(true, "http://test1", properties1);
		s1.setId("1");
		subscribers.add(s1);
		final Map<String, String> properties2 = new HashMap<>();
		Subscriber s2 = new Subscriber(true, "http://test2", properties2);
		s2.setId("2");
		subscribers.add(s2);
		final AtomicInteger count = new AtomicInteger(0);
		SubscriberManager manager = new CommonSubscriberManager(Person.class, subscribers) {
			@Override
			CommonMessenger createMessenger() {
				if (count.getAndIncrement() == 0) {
					return messenger1;
				} else
					return messenger2;
			}
		};

		Assert.assertTrue(manager.hasEnabledSubscribers());

		Map<Subscriber, Messenger> messengers = Deencapsulation.getField(manager, "messengers");
		Assert.assertEquals(2, messengers.size());
		manager.dispose();
		Assert.assertEquals(0, messengers.size());

		Assert.assertFalse(manager.hasEnabledSubscribers());

		new VerificationsInOrder() {
			{
				messenger1.init(withSameInstance(Person.class), withEqual(new URI("http://test1")), withSameInstance(properties1));
				times = 1;

				messenger2.init(withSameInstance(Person.class), withEqual(new URI("http://test2")), withSameInstance(properties2));
				times = 1;

				messenger2.dispose();
				times = 1;

				messenger1.dispose();
				times = 1;
			}
		};
	}

	@Test
	public void disposeWithoutCancellation(final @Mocked CommonMessenger messenger1, final @Mocked CommonMessenger messenger2) throws Exception {
		List<Subscriber> subscribers = new ArrayList<>();
		final Map<String, String> properties1 = new HashMap<>();
		Subscriber s1 = new Subscriber(true, "http://test1", properties1);
		s1.setId("1");
		subscribers.add(s1);
		final AtomicInteger count = new AtomicInteger(0);
		SubscriberManager manager = new CommonSubscriberManager(Person.class, subscribers) {
			@Override
			CommonMessenger createMessenger() {
				if (count.getAndIncrement() == 0) {
					return messenger1;
				} else
					return messenger2;
			}
		};

		final Map<String, String> properties2 = new HashMap<>();
		final AtomicReference<Person> received = new AtomicReference(null);
		final AtomicBoolean cancelled = new AtomicBoolean(false);
		final SubscriberListener listener = new SubscriberListener(new MessageReceiver() {
			@Override
			public void receive(Object message) {
				received.set((Person) message);
			}

			@Override
			public void cancel() {
				cancelled.set(true);
			}
		}, properties2);
		manager.add(listener);

		Assert.assertTrue(manager.hasEnabledSubscribers());
		Assert.assertTrue(manager.hasListeners());

		Map<Subscriber, Messenger> messengers = Deencapsulation.getField(manager, "messengers");
		Assert.assertEquals(2, messengers.size());
		manager.dispose(false);
		Assert.assertEquals(1, messengers.size());

		Assert.assertNull(received.get());
		Assert.assertFalse(cancelled.get());

		Assert.assertTrue(manager.hasEnabledSubscribers());
		Assert.assertTrue(manager.hasListeners());

		new VerificationsInOrder() {
			{
				messenger1.init(withSameInstance(Person.class), withEqual(new URI("http://test1")), withSameInstance(properties1));
				times = 1;

				messenger2.init(withSameInstance(Person.class), withEqual(listener), withSameInstance(properties2));
				times = 1;

				messenger1.dispose();
				times = 1;

				messenger2.dispose();
				times = 0;
			}
		};
	}

	@Test
	public void disposeWithCancellation() throws Exception {
		SubscriberManager manager = new CommonSubscriberManager(Person.class, new ArrayList<Subscriber>());
		final AtomicReference<Person> received = new AtomicReference(null);
		final AtomicBoolean cancelled = new AtomicBoolean(false);
		final SubscriberListener listener = new SubscriberListener(new MessageReceiver() {
			@Override
			public void receive(Object message) {
				received.set((Person) message);
			}

			@Override
			public void cancel() {
				cancelled.set(true);
			}
		});
		manager.add(listener);

		Assert.assertTrue(manager.hasEnabledSubscribers());
		Assert.assertTrue(manager.hasListeners());

		Map<Subscriber, Messenger> messengers = Deencapsulation.getField(manager, "messengers");
		Assert.assertEquals(1, messengers.size());
		manager.dispose(true);
		Assert.assertEquals(0, messengers.size());

		Assert.assertNull(received.get());
		Assert.assertTrue(cancelled.get());

		Assert.assertFalse(manager.hasEnabledSubscribers());
		Assert.assertFalse(manager.hasListeners());
	}

	@Test
	public void testCleanup(final @Mocked CommonMessenger messenger, final @Mocked FutureSendTask task1, final @Mocked FutureSendTask task2,
			final @Mocked FutureSendTask task3) throws Exception {
		List<Subscriber> subscribers = new ArrayList<>();
		final Map<String, String> properties1 = new HashMap<>();
		Subscriber s1 = new Subscriber(true, "http://test1", properties1);
		s1.setId("1");
		subscribers.add(s1);
		SubscriberManager manager = new CommonSubscriberManager(String.class, subscribers);

		new NonStrictExpectations() {
			{
				messenger.send(withEqual("1"));
				result = task1;

				task1.isDone();
				result = true;

				messenger.send(withEqual("2"));
				result = task2;

				task2.isDone();
				result = true;

				messenger.send(withEqual("3"));
				result = task3;

				task3.isDone();
				result = true;
			}
		};

		manager.send("1");
		manager.send("2");
		manager.send("3");

		Thread.sleep(500);

		new VerificationsInOrder() {
			{
				task1.isDone();
				times = 1;

				task1.get();
				times = 1;

				task2.isDone();
				times = 1;

				task2.get();
				times = 1;

				task3.isDone();
				times = 1;

				task3.get();
				times = 1;
			}
		};
	}
}
