package havis.transport.common;

import static org.junit.Assert.assertEquals;
import havis.transport.Callback;
import havis.transport.FutureSendTask;
import havis.transport.Marshaller;
import havis.transport.Messenger;
import havis.transport.Subscriber;
import havis.transport.Subscriptor;
import havis.transport.SubscriptorManager;
import havis.transport.TransportException;
import havis.transport.ValidationException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import mockit.Mocked;
import mockit.NonStrictExpectations;

import org.junit.Assert;
import org.junit.Test;

public class CommonSubscriptorManagerTest {

	@Test
	public void init(final @Mocked CommonSubscriberManager manager) throws Exception {

		SubscriptorManager subManager = new CommonSubscriptorManager(manager, new Callback() {
			@Override
			public void arrived(String path, Object message) {
			}
		}, Arrays.asList(new Subscriptor(false, "foo", "bar", "whatever")));
		Assert.assertEquals(subManager.getSubscriptors().size(), 0);
		
		
		Subscriptor notUnique1 = new Subscriptor(false, "foo", "bar", "whatever");
		notUnique1.setId("notUnique");
		Subscriptor notUnique2 = new Subscriptor(false, "foo", "bar", "whatever");
		notUnique2.setId("notUnique");
		subManager = new CommonSubscriptorManager(manager, null, Arrays.asList(notUnique1, notUnique2));
		Assert.assertEquals(subManager.getSubscriptors().size(), 1);

		List<Subscriptor> subscriptors = new ArrayList<>();
		final Map<String, String> properties1 = new HashMap<>();
		Subscriptor s1 = new Subscriptor(false, "foo", "bar", "whatever", properties1);
		s1.setId("1");
		subscriptors.add(s1);
		final Map<String, String> properties2 = new HashMap<>();
		Subscriptor s2 = new Subscriptor(false, "foo", "bar", "whatever", properties2);
		s2.setId("2");
		subscriptors.add(s2);
		final Map<String, String> properties3 = new HashMap<>();
		Subscriptor s3 = new Subscriptor(false, "foo", "bar", "whatever", properties3);
		s3.setEnable(true);
		s3.setId("3");
		subscriptors.add(s3);
		new NonStrictExpectations() {
			{
				manager.get(withAny(""));
				Subscriber s = new Subscriber(true, "http://whatever");
				s.setId("whatever");
				result = s;
			}
		};
		subManager = new CommonSubscriptorManager(manager, null, subscriptors);
		Assert.assertEquals(3, subscriptors.size());
		Assert.assertSame(s1, subscriptors.get(0));
		Assert.assertSame(s2, subscriptors.get(1));
		Assert.assertSame(s3, subscriptors.get(2));

		new NonStrictExpectations() {
			{
				manager.get(withAny(""));
				Subscriber s = new Subscriber(false, "http://whatever");
				s.setId("whatever");
				result = s;
			}
		};
		subManager = new CommonSubscriptorManager(manager, null, subscriptors);

		new NonStrictExpectations() {
			{
				manager.get(withAny(""));
				result = null;
			}
		};
		subManager = new CommonSubscriptorManager(manager, null, subscriptors);

	}

	@Test
	public void add(final @Mocked CommonSubscriberManager manager, final @Mocked CommonMessenger<Person> messenger)
			throws TransportException, ValidationException, URISyntaxException {
		SubscriptorManager subManager = new CommonSubscriptorManager(manager);
		Subscriptor subscriptor = new Subscriptor();

		try {
			subManager.add(null);
			Assert.fail("Expected NullPointerException");
		} catch (NullPointerException e) {
			// ok
		}

		try {
			subManager.add(subscriptor);
			Assert.fail("Expected ValidationException");
		} catch (ValidationException e) {
			// ok
		}

		subscriptor.setId("foo");
		try {
			subManager.add(subscriptor);
			Assert.fail("Expected ValidationException");
		} catch (ValidationException e) {
			// ok
		}

		new NonStrictExpectations() {
			{
				manager.get(withAny(""));
				Subscriber s = new Subscriber(false, "http://whatever");
				s.setId("whatever");
				result = s;
			}
		};

		subscriptor.setId(null);
		subscriptor.setEnable(true);
		subscriptor.setSubscriberId("whatever");
		try {
			subManager.add(subscriptor);
			Assert.fail("Expected ValidationException");
		} catch (ValidationException e) {
			// ok
		}

		new NonStrictExpectations() {
			{
				manager.get(withAny(""));
				result = null;
			}
		};
		subscriptor.setId(null);
		subscriptor.setSubscriberId("foo");
		try {
			subManager.add(subscriptor);
			Assert.fail("Expected ValidationException");
		} catch (ValidationException e) {
			// ok
		}

		subscriptor.setSubscriberId("whatever");
		try {
			subManager.add(subscriptor);
			Assert.fail("Expected ValidationException");
		} catch (ValidationException e) {
			// ok
		}

		new NonStrictExpectations() {
			{
				manager.get(withAny(""));
				Subscriber s = new Subscriber(true, "http://whatever");
				s.setId("whatever");
				result = s;
			}
		};

		subManager = new CommonSubscriptorManager(manager, new Callback() {
			@Override
			public void arrived(String path, Object msg) {
			}
		});
		subscriptor.setEnable(true);
		subManager.add(subscriptor);

		subscriptor.setId(null);
		subscriptor.setPath("");
		subManager.add(subscriptor);
		subscriptor.setId(null);
		subscriptor.setPath("foo");
		subManager.add(subscriptor);

		new NonStrictExpectations() {
			{
				messenger.getUri();
				result = new URI("http://whatever/bar");
			}
		};
		subscriptor.setId(null);
		subManager.add(subscriptor);

		new NonStrictExpectations() {
			{
				messenger.getUri();
				result = new URI("http://whatever/");
			}
		};
		subscriptor.setId(null);
		subManager.add(subscriptor);

		new NonStrictExpectations() {
			{
				messenger.getUri();
				result = new URI("http://whatever");
			}
		};
		subscriptor.setId(null);
		subscriptor.setPath("/bar");
		subManager.add(subscriptor);

	}

	@Test
	public void remove(final @Mocked CommonSubscriberManager manager) throws TransportException, ValidationException {
		SubscriptorManager subManager = new CommonSubscriptorManager(manager, new Callback() {
			@Override
			public void arrived(String path, Object msg) {

			}
		});
		Subscriptor subscriptor = new Subscriptor();
		subscriptor.setEnable(true);
		subscriptor.setSubscriberId("whatever");

		new NonStrictExpectations() {
			{
				manager.get(withAny(""));
				Subscriber s = new Subscriber(true, "http://whatever");
				s.setId("whatever");
				result = s;
			}
		};
		subManager.add(subscriptor);

		subManager.remove(subscriptor.getId());

		subscriptor.setId(null);
		try {
			subManager.remove(subscriptor);
			Assert.fail("Expected ValidationException");
		} catch (ValidationException e) {
			// ok
		}

		try {
			subManager.remove((Subscriptor) null);
			Assert.fail("Expected NullPointerException");
		} catch (NullPointerException e) {
			// ok
		}

		subscriptor.setId("Foo");
		try {
			subManager.remove(subscriptor);
			Assert.fail("Expected ValidationException");
		} catch (ValidationException e) {
			// ok
		}

	}

	@Test
	public void getSubscriptors(final @Mocked CommonSubscriberManager manager) throws TransportException, ValidationException {
		SubscriptorManager subManager = new CommonSubscriptorManager(manager);
		Subscriptor subscriptor = new Subscriptor();
		subscriptor.setSubscriberId("whatever");
		subManager.add(subscriptor);

		assertEquals(subManager.getSubscriptors().size(), 1);
		assertEquals(subManager.getSubscriptors().contains(subscriptor), true);
	}

	@Test
	public void dispose(final @Mocked CommonSubscriberManager manager) throws TransportException, ValidationException {
		SubscriptorManager subManager = new CommonSubscriptorManager(manager);
		Subscriptor subscriptor = new Subscriptor();
		subscriptor.setSubscriberId("whatever");
		subManager.add(subscriptor);

		subManager.dispose();
	}

	@Test
	public void update(final @Mocked CommonSubscriberManager manager) throws TransportException, ValidationException {
		new NonStrictExpectations() {
			{
				manager.get(withAny(""));
				Subscriber s = new Subscriber(true, "http://whatever");
				s.setId("whatever");
				result = s;
			}
		};

		SubscriptorManager subManager = new CommonSubscriptorManager(manager);
		Subscriptor subscriptor = new Subscriptor();
		subscriptor.setSubscriberId("whatever");
		subscriptor.setEnable(true);
		subManager.add(subscriptor);

		subscriptor.setPath("hugo");
		subManager.update(subscriptor);

		Subscriptor newSub = new Subscriptor();
		newSub.setId(subscriptor.getId());
		newSub.setSubscriberId("whatever");
		newSub.setEnable(false);
		subManager.update(newSub);

		newSub = new Subscriptor();
		newSub.setId(subscriptor.getId());
		newSub.setSubscriberId("whatever");
		newSub.setEnable(true);
		subManager.update(newSub);

		newSub = new Subscriptor();
		newSub.setId(subscriptor.getId());
		newSub.setSubscriberId("whatever");
		newSub.setEnable(false);
		subManager.update(newSub);

		new NonStrictExpectations() {
			{
				manager.get(withAny(""));
				Subscriber s = new Subscriber(false, "http://whatever");
				s.setId("whatever");
				result = s;
			}
		};
		subManager = new CommonSubscriptorManager(manager);
		subscriptor = new Subscriptor();
		subscriptor.setSubscriberId("whatever");
		subscriptor.setEnable(false);
		subManager.add(subscriptor);
		newSub = new Subscriptor();
		newSub.setId(subscriptor.getId());
		newSub.setSubscriberId("whatever");
		newSub.setEnable(true);

		try {
			subManager.update(newSub);
			Assert.fail("Expected TransportException");
		} catch (TransportException e) {
			// ok
		}

		subManager.dispose();
	}

	@Test
	public void send(final @Mocked CommonSubscriberManager manager) throws TransportException, ValidationException {
		new NonStrictExpectations() {
			{
				manager.get(withAny(""));
				Subscriber s = new Subscriber(true, "http://whatever");
				s.setId("whatever");
				result = s;
			}
		};

		SubscriptorManager subManager = new CommonSubscriptorManager(manager);
		Subscriptor subscriptor = new Subscriptor();
		subscriptor.setSubscriberId("whatever");
		subscriptor.setEnable(true);
		subManager.add(subscriptor);
		subManager.send(new Person("Foo"));

	}
	
	@SuppressWarnings("rawtypes")
	@Test
	public void testCancellationRemove(final @Mocked HttpURLConnection connection, final @Mocked URL url, final @Mocked SSLContextManager ssl, final @Mocked StreamFactory streamFactory) throws Exception {
		List<Subscriber> subscribers = new ArrayList<>();
		final Map<String, String> properties1 = new HashMap<>();
		properties1.put(Messenger.TCP_TIMEOUT_PROPERTY, "10000");
		Subscriber s1 = new Subscriber(true, "http://test1", properties1);
		s1.setId("1");
		subscribers.add(s1);
		CommonSubscriberManager manager = new CommonSubscriberManager(String.class, subscribers);
		SubscriptorManager subManager = new CommonSubscriptorManager(manager);
		Subscriptor subscriptor = new Subscriptor();
		subscriptor.setPath("path");
		subscriptor.setSubscriberId("1");
		subscriptor.setEnable(true);
		String id = subManager.add(subscriptor);

		new NonStrictExpectations() {
			{
				streamFactory.getMarshaller();
				result = new Marshaller<String>() {
					@Override
					public void marshal(String arg0, OutputStream arg1) throws TransportException {
						try {
							arg1.write(1);
						} catch (IOException e) {
							// ignore
						}
					}

					@Override
					public String unmarshal(InputStream arg0) throws TransportException {
						return null;
					}
				};

				connection.getOutputStream();
				result = new OutputStream() {
					@Override
					public void write(int b) throws IOException {
						Thread.interrupted();
						CountDownLatch latch = new CountDownLatch(1);
						try {
							latch.await();
						} catch (InterruptedException e) {
							return;
						}
					}
				};
				
				connection.getResponseCode();
				result = 200;
				
				connection.getHeaderFields();
				result = new HashMap<String, List<String>>();
			}
		};

		FutureSendTask t1 = subManager.send("1").get(subscriptor);
		FutureSendTask t2 = subManager.send("2").get(subscriptor);
		FutureSendTask t3 = subManager.send("3").get(subscriptor);
		
		Thread.sleep(100);

		Assert.assertFalse(t1.isDone());
		Assert.assertFalse(t1.isCancelled());
		Assert.assertFalse(t2.isDone());
		Assert.assertFalse(t2.isCancelled());
		Assert.assertFalse(t3.isDone());
		Assert.assertFalse(t3.isCancelled());

		subManager.remove(id);

		Thread.sleep(500);

		Assert.assertTrue(t1.isDone());
		Assert.assertTrue(t1.isCancelled());
		Assert.assertTrue(t2.isDone());
		Assert.assertTrue(t2.isCancelled());
		Assert.assertTrue(t3.isDone());
		Assert.assertTrue(t3.isCancelled());
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testCancellationUpdate(final @Mocked HttpURLConnection connection, final @Mocked URL url, final @Mocked SSLContextManager ssl, final @Mocked StreamFactory streamFactory) throws Exception {
		List<Subscriber> subscribers = new ArrayList<>();
		final Map<String, String> properties1 = new HashMap<>();
		properties1.put(Messenger.TCP_TIMEOUT_PROPERTY, "10000");
		Subscriber s1 = new Subscriber(true, "http://test1", properties1);
		s1.setId("1");
		subscribers.add(s1);
		CommonSubscriberManager manager = new CommonSubscriberManager(String.class, subscribers);
		SubscriptorManager subManager = new CommonSubscriptorManager(manager);
		Subscriptor subscriptor = new Subscriptor();
		subscriptor.setPath("path");
		subscriptor.setSubscriberId("1");
		subscriptor.setEnable(true);
		String id = subManager.add(subscriptor);

		new NonStrictExpectations() {
			{
				streamFactory.getMarshaller();
				result = new Marshaller<String>() {
					@Override
					public void marshal(String arg0, OutputStream arg1) throws TransportException {
						try {
							arg1.write(1);
						} catch (IOException e) {
							// ignore
						}
					}

					@Override
					public String unmarshal(InputStream arg0) throws TransportException {
						return null;
					}
				};

				connection.getOutputStream();
				result = new OutputStream() {
					@Override
					public void write(int b) throws IOException {
						Thread.interrupted();
						CountDownLatch latch = new CountDownLatch(1);
						try {
							latch.await();
						} catch (InterruptedException e) {
							return;
						}
					}
				};
				
				connection.getResponseCode();
				result = 200;
				
				connection.getHeaderFields();
				result = new HashMap<String, List<String>>();
			}
		};

		FutureSendTask t1 = subManager.send("1").get(subscriptor);
		FutureSendTask t2 = subManager.send("2").get(subscriptor);
		FutureSendTask t3 = subManager.send("3").get(subscriptor);
		
		Thread.sleep(100);

		Assert.assertFalse(t1.isDone());
		Assert.assertFalse(t1.isCancelled());
		Assert.assertFalse(t2.isDone());
		Assert.assertFalse(t2.isCancelled());
		Assert.assertFalse(t3.isDone());
		Assert.assertFalse(t3.isCancelled());

		Subscriptor u1 = new Subscriptor();
		u1.setId(id);
		u1.setPath("path");
		u1.setSubscriberId("1");
		u1.setEnable(false);
		subManager.update(u1);

		Thread.sleep(500);

		Assert.assertTrue(t1.isDone());
		Assert.assertTrue(t1.isCancelled());
		Assert.assertTrue(t2.isDone());
		Assert.assertTrue(t2.isCancelled());
		Assert.assertTrue(t3.isDone());
		Assert.assertTrue(t3.isCancelled());
	}

}