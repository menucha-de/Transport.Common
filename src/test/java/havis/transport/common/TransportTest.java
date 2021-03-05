package havis.transport.common;

import havis.middleware.ale.service.ec.ECReports;
import havis.transform.common.JsTransformerFactory;
import havis.transport.Messenger;
import havis.transport.TransportConnectionException;
import havis.transport.ValidationException;
import havis.util.monitor.Broker;
import havis.util.monitor.Event;
import havis.util.monitor.Source;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Providers;
import javax.ws.rs.ext.RuntimeDelegate;

import org.jboss.resteasy.plugins.providers.RegisterBuiltin;
import org.jboss.resteasy.spi.ResteasyProviderFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TransportTest {

	@BeforeClass
	public static void init() {
		final Providers providers = (Providers) RuntimeDelegate.getInstance();
		RegisterBuiltin.register((ResteasyProviderFactory) providers);
		Provider.createFactory(new Provider() {
			@Override
			public <T> MessageBodyReader<T> getMessageBodyReader(Class<T> clazz, Type type, Annotation[] annotations, MediaType mediaType) {
				ResteasyProviderFactory.pushContext(Providers.class, providers);
				return providers.getMessageBodyReader(clazz, type, annotations, mediaType);
			}

			@Override
			public <T> T read(MessageBodyReader<T> reader, Class<T> clazz, Type type, Annotation[] annotations, MediaType mediaType,
					MultivaluedMap<String, String> properties, InputStream stream) throws Exception {
				ResteasyProviderFactory.pushContext(Providers.class, providers);
				return reader.readFrom(clazz, type, annotations, mediaType, properties, stream);
			}

			@Override
			public <T> MessageBodyWriter<T> getMessageBodyWriter(Class<T> clazz, Type type, Annotation[] annotations, MediaType mediaType) {
				ResteasyProviderFactory.pushContext(Providers.class, providers);
				return providers.getMessageBodyWriter(clazz, type, annotations, mediaType);
			}

			@Override
			public <T> void write(MessageBodyWriter<T> writer, T data, Class<?> clazz, Type type, Annotation[] annotations, MediaType mediaType,
					MultivaluedMap<String, Object> properties, OutputStream stream) throws Exception {
				ResteasyProviderFactory.pushContext(Providers.class, providers);
				writer.writeTo(data, clazz, type, annotations, mediaType, properties, stream);
			}
		});
		Connector.createFactory(new Connector() {
			@SuppressWarnings("unchecked")
			@Override
			public <S> S newInstance(Class<S> clazz, String type) throws ValidationException {
				if ("javascript".equals(type)) {
					return (S) new JsTransformerFactory().newInstance();
				}
				return null;
			}

			@Override
			public <S> List<String> getTypes(Class<S> clazz) throws ValidationException {
				return null;
			}

			@Override
			public Broker getBroker() {
				return new Broker() {
					@Override
					public void notify(Source arg0, Event arg1) {
					}
				};
			}
		});

		Logger root = Logger.getLogger("");
		root.setLevel(Level.FINEST);
		for (Handler handler : root.getHandlers()) {
			handler.setLevel(Level.FINEST);
		}
	}

	@Test
	public void jsonJavascriptTest() throws Exception {
		final CountDownLatch ready = new CountDownLatch(1);
		final CountDownLatch signal = new CountDownLatch(1);
		try (ServerSocket socket = new ServerSocket()) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						socket.bind(null);
						ready.countDown();
						try (Socket s = socket.accept()) {
							try (InputStream stream = s.getInputStream()) {
								byte[] bytes = new byte[4092];
								int len = stream.read(bytes);
								Assert.assertEquals("\"hurz\"", new String(bytes, 0, len));
							}
						}
						signal.countDown();
					} catch (IOException e) {
						e.printStackTrace();
						Assert.fail();
					}
				}
			}).start();

			if (!ready.await(100, TimeUnit.MILLISECONDS)) {
				Assert.fail();
			}

			Map<String, String> properties = new HashMap<>();
			properties.put(Messenger.TRANSFORMER_PROPERTY, Messenger.JS_TRANSFORMER);
			properties.put(Messenger.JS_SCRIPT_PROPERTY, "return object.getName();");
			properties.put(Messenger.MIMETYPE_PROPERTY, "application/json");
			Messenger<Person> messenger = new CommonMessenger<>();
			messenger.init(Person.class, new URI("tcp://localhost:" + socket.getLocalPort()), properties);
			messenger.send(new Person("hurz")).get();
			if (!signal.await(1, TimeUnit.SECONDS))
				Assert.fail();
		}
	}
	
	@Test
	public void resendTest() throws Exception {
		final CountDownLatch ready = new CountDownLatch(1);
		final CountDownLatch signal1 = new CountDownLatch(1);
		final CountDownLatch signal2 = new CountDownLatch(1);
		final CountDownLatch signal3 = new CountDownLatch(1);
		final CountDownLatch signal4 = new CountDownLatch(1);
		try (ServerSocket socket = new ServerSocket()) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						ready.await();
						socket.bind(new InetSocketAddress(12345));
						try (Socket s = socket.accept()) {
							try (InputStream stream = s.getInputStream()) {
								byte[] bytes = new byte[4092];
								int len = stream.read(bytes);
								Assert.assertEquals("1", new String(bytes, 0, len));
							}
						}
						signal1.countDown();
						try (Socket s = socket.accept()) {
							try (InputStream stream = s.getInputStream()) {
								byte[] bytes = new byte[4092];
								int len = stream.read(bytes);
								Assert.assertEquals("2", new String(bytes, 0, len));
							}
						}
						signal2.countDown();
						try (Socket s = socket.accept()) {
							try (InputStream stream = s.getInputStream()) {
								byte[] bytes = new byte[4092];
								int len = stream.read(bytes);
								Assert.assertEquals("3", new String(bytes, 0, len));
							}
						}
						signal3.countDown();
						try (Socket s = socket.accept()) {
							try (InputStream stream = s.getInputStream()) {
								byte[] bytes = new byte[4092];
								int len = stream.read(bytes);
								Assert.assertEquals("4", new String(bytes, 0, len));
							}
						}
						signal4.countDown();
						
						for (int i = 5; i <= 8; i++) {
							try (Socket s = socket.accept()) {
								try (InputStream stream = s.getInputStream()) {
									byte[] bytes = new byte[4092];
									int len = stream.read(bytes);
									Assert.assertEquals(Integer.toString(i), new String(bytes, 0, len));
								}
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
						Assert.fail();
					}
				}
			}).start();

			Map<String, String> properties = new HashMap<>();
			properties.put(Messenger.MIMETYPE_PROPERTY, "text/plain");
			properties.put(Messenger.RESEND_REPEAT_PERIOD_PROPERTY, "1000");
			properties.put(Messenger.RESEND_QUEUE_SIZE_PROPERTY, "100");
			Messenger<String> messenger = new CommonMessenger<>();
			messenger.init(String.class, new URI("tcp://localhost:12345"), properties);
			messenger.send("1");
			messenger.send("2");
			messenger.send("3");
			messenger.send("4");

			Thread.sleep(2000);

			ready.countDown();

			if (!signal1.await(5, TimeUnit.SECONDS))
				Assert.fail();

			if (!signal2.await(5, TimeUnit.SECONDS))
				Assert.fail();

			if (!signal3.await(5, TimeUnit.SECONDS))
				Assert.fail();

			if (!signal4.await(5, TimeUnit.SECONDS))
				Assert.fail();
			
			Thread.sleep(1000);
			
			messenger.send("5");

			Thread.sleep(1000);

			messenger.send("6");

			Thread.sleep(1000);

			messenger.send("7");

			Thread.sleep(1000);
			messenger.send("8");

			Thread.sleep(1000);
		}
	}

	@Test
	public void resendDiscardTest() throws Exception {
		final CountDownLatch ready = new CountDownLatch(1);
		final CountDownLatch signal1 = new CountDownLatch(1);
		final CountDownLatch signal2 = new CountDownLatch(1);
		try (ServerSocket socket = new ServerSocket()) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						ready.await();
						socket.bind(new InetSocketAddress(12345));
						try (Socket s = socket.accept()) {
							try (InputStream stream = s.getInputStream()) {
								byte[] bytes = new byte[4092];
								int len = stream.read(bytes);
								Assert.assertEquals("1", new String(bytes, 0, len));
							}
						}
						signal1.countDown();
						try (Socket s = socket.accept()) {
							try (InputStream stream = s.getInputStream()) {
								byte[] bytes = new byte[4092];
								int len = stream.read(bytes);
								Assert.assertEquals("2", new String(bytes, 0, len));
							}
						}
						signal2.countDown();
					} catch (Exception e) {
						e.printStackTrace();
						Assert.fail();
					}
				}
			}).start();

			Map<String, String> properties = new HashMap<>();
			properties.put(Messenger.MIMETYPE_PROPERTY, "text/plain");
			properties.put(Messenger.RESEND_REPEAT_PERIOD_PROPERTY, "1000");
			properties.put(Messenger.RESEND_QUEUE_SIZE_PROPERTY, "1");
			Messenger<String> messenger = new CommonMessenger<>();
			messenger.init(String.class, new URI("tcp://localhost:12345"), properties);
			messenger.send("1");
			Thread.sleep(100);
			messenger.send("2");
			Thread.sleep(100);
			messenger.send("3").get();

			Thread.sleep(2000);

			ready.countDown();

			if (!signal1.await(5, TimeUnit.SECONDS))
				Assert.fail();

			if (!signal2.await(5, TimeUnit.SECONDS))
				Assert.fail();
		}
	}

	@Test
	public void connectionErrorWithResendTest() throws Exception {
		final CountDownLatch ready = new CountDownLatch(1);
		final CountDownLatch next = new CountDownLatch(1);

		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					ready.await();
					try (ServerSocket socket = new ServerSocket()) {
						socket.bind(new InetSocketAddress(12345));
						try (Socket s = socket.accept()) {
							try (InputStream stream = s.getInputStream()) {
								byte[] bytes = new byte[4092];
								int len = stream.read(bytes);
								Assert.assertEquals("1", new String(bytes, 0, len));
							}
						}
					}
					next.await();
					try (ServerSocket socket = new ServerSocket()) {
						socket.bind(new InetSocketAddress(12345));
						try (Socket s = socket.accept()) {
							try (InputStream stream = s.getInputStream()) {
								byte[] bytes = new byte[4092];
								int len = stream.read(bytes);
								Assert.assertEquals("2", new String(bytes, 0, len));
							}
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
					Assert.fail();
				}
			}
		}).start();

			Map<String, String> properties = new HashMap<>();
			properties.put(Messenger.MIMETYPE_PROPERTY, "text/plain");
			properties.put(Messenger.RESEND_REPEAT_PERIOD_PROPERTY, "1000");
			CommonMessenger<String> messenger = new CommonMessenger<>();
			messenger.connectionErrorThresholdMs = 3000;
			messenger.init(String.class, new URI("tcp://localhost:12345"), properties);

			ready.countDown();
			messenger.send("1").get();

			Thread.sleep(1000);

			// queued
			messenger.send("2");

			Thread.sleep(3000);

			next.countDown();
			Thread.sleep(100);
		}

		@Test
		public void connectionErrorTest() throws Exception {
			final CountDownLatch ready = new CountDownLatch(1);
			final CountDownLatch next = new CountDownLatch(1);

			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						ready.await();
						try (ServerSocket socket = new ServerSocket()) {
							socket.bind(new InetSocketAddress(12345));
							try (Socket s = socket.accept()) {
								try (InputStream stream = s.getInputStream()) {
									byte[] bytes = new byte[4092];
									int len = stream.read(bytes);
									Assert.assertEquals("1", new String(bytes, 0, len));
								}
							}
						}
						next.await();
						try (ServerSocket socket = new ServerSocket()) {
							socket.bind(new InetSocketAddress(12345));
							try (Socket s = socket.accept()) {
								try (InputStream stream = s.getInputStream()) {
									byte[] bytes = new byte[4092];
									int len = stream.read(bytes);
									Assert.assertEquals("2", new String(bytes, 0, len));
								}
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
						Assert.fail();
					}
				}
			}).start();

			Map<String, String> properties = new HashMap<>();
			properties.put(Messenger.MIMETYPE_PROPERTY, "text/plain");
			CommonMessenger<String> messenger = new CommonMessenger<>();
			messenger.connectionErrorThresholdMs = 3000;
			messenger.init(String.class, new URI("tcp://localhost:12345"), properties);

			ready.countDown();
			messenger.send("1").get();

			Thread.sleep(1000);

			try {
				messenger.send("2").get();
				Assert.fail("Expected TransportConnectionException");
			} catch (TransportConnectionException e) {
				// expected
			}

			Thread.sleep(3000);

			try {
				messenger.send("2").get();
				Assert.fail("Expected TransportConnectionException");
			} catch (TransportConnectionException e) {
				// expected
			}

			Thread.sleep(100);

			try {
				messenger.send("2").get();
				Assert.fail("Expected TransportConnectionException");
			} catch (TransportConnectionException e) {
				// expected
			}

			next.countDown();
			messenger.send("2").get();

			Thread.sleep(100);
	}

	@Test
	public void plainXmlTest() throws Exception {
		final CountDownLatch ready = new CountDownLatch(1);
		final CountDownLatch signal = new CountDownLatch(2);
		try (ServerSocket socket = new ServerSocket()) {
			new Thread(new Runnable() {

				void read(int index) throws IOException {
					Socket s = socket.accept();
					try {
						InputStream stream = s.getInputStream();
						try {
							byte[] bytes = new byte[4092];
							int len = stream.read(bytes);
							Assert.assertEquals(
									"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><Holder><value xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xsi:type=\"xs:string\">test"
											+ index + "</value></Holder>",
									new String(bytes, 0, len));
						} finally {
							stream.close();
						}
					} finally {
						s.close();
					}
					signal.countDown();
				}

				@Override
				public void run() {
					try {
						socket.bind(null);
						ready.countDown();
						read(1);
						read(2);
					} catch (IOException e) {
						e.printStackTrace();
						Assert.fail();
					}
				}
			}).start();

			if (!ready.await(100, TimeUnit.MILLISECONDS)) {
				Assert.fail();
			}

			@SuppressWarnings("rawtypes")
			Messenger<Holder> messenger = new CommonMessenger<>();
			messenger.init(Holder.class, new URI("tcp://localhost:" + socket.getLocalPort()), null);
			messenger.send(new Holder<String>("test1")).get();
			messenger.send(new Holder<String>("test2")).get();
			if (!signal.await(2, TimeUnit.SECONDS))
				Assert.fail();
		}
	}

	@Test
	public void udpTest() throws Exception {
		final CountDownLatch ready = new CountDownLatch(1);
		final CountDownLatch signal = new CountDownLatch(1);
		try (DatagramSocket socket = new DatagramSocket();) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						byte[] receiveData = new byte[4];
						DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
						ready.countDown();
						socket.receive(receivePacket);
						String result = new String(receivePacket.getData(), 0, receivePacket.getLength());
						Assert.assertEquals("hurz", result);
						signal.countDown();
					} catch (IOException e) {
						e.printStackTrace();
						Assert.fail();
					}
				}
			}).start();

			if (!ready.await(100, TimeUnit.MILLISECONDS)) {
				Assert.fail();
			}

			Map<String, String> properties = new HashMap<>();
			properties.put(Messenger.MIMETYPE_PROPERTY, "text/plain");
			Messenger<String> messenger = new CommonMessenger<>();
			messenger.init(String.class, new URI("udp://localhost:" + socket.getLocalPort()), properties);
			messenger.send("hurz").get();
			if (!signal.await(1, TimeUnit.SECONDS))
				Assert.fail();
		}
	}

	@Test
	public void binaryUdpTest() throws Exception {
		final byte[] message = new byte[] { 0x00, 0x01, 0x02 };
		final CountDownLatch ready = new CountDownLatch(1);
		final CountDownLatch signal = new CountDownLatch(1);
		try (DatagramSocket socket = new DatagramSocket();) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						byte[] receiveData = new byte[message.length];
						DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
						ready.countDown();
						socket.receive(receivePacket);
						Assert.assertArrayEquals(message, receivePacket.getData());
						signal.countDown();
					} catch (IOException e) {
						e.printStackTrace();
						Assert.fail();
					}
				}
			}).start();

			if (!ready.await(100, TimeUnit.MILLISECONDS)) {
				Assert.fail();
			}

			Map<String, String> properties = new HashMap<>();
			Messenger<byte[]> messenger = new CommonMessenger<>();
			messenger.init(byte[].class, new URI("udp://localhost:" + socket.getLocalPort()), properties);
			messenger.send(message).get();

			if (!signal.await(1, TimeUnit.SECONDS))
				Assert.fail();
		}
	}

	@Test
	public void httpTest() throws Exception {
		final CountDownLatch ready = new CountDownLatch(1);
		final CountDownLatch signal = new CountDownLatch(1);
		try (ServerSocket socket = new ServerSocket()) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						socket.bind(null);
						ready.countDown();
						try (Socket s = socket.accept()) {
							try (PrintWriter request = new PrintWriter(s.getOutputStream())) {
								try (BufferedReader response = new BufferedReader(new InputStreamReader(s.getInputStream()))) {
									String responseLine;
									boolean first = true;
									while ((responseLine = response.readLine()) != null && !responseLine.equals("")) {
										if (first) {
											first = false;
											Assert.assertEquals("POST / HTTP/1.1", responseLine);
										} else {
											int index = responseLine.indexOf(':');
											if (index < 0)
												Assert.fail("Expected HTTP header, but got " + responseLine);
											String key = responseLine.substring(0, index).toLowerCase();
											String value = responseLine.substring(index + 1, responseLine.length()).trim();
											switch (key) {
											case "content-type":
												Assert.assertEquals("application/json", value);
												break;
											case "authorization":
												Assert.assertEquals("Basic YWRtaW46YWRtaW4=", value);
												break;
											default:
												break;
											}
										}
									}

									request.print("HTTP/1.1 200 OK\r\n\r\n");
									request.flush();

									while ((responseLine = response.readLine()) != null) {
										Assert.assertEquals("{\"name\":\"Peter\"}", responseLine);
									}
								}
							}
						}
						signal.countDown();
					} catch (Exception e) {
						e.printStackTrace();
						Assert.fail();
					}
				}
			}).start();

			if (!ready.await(100, TimeUnit.MILLISECONDS)) {
				Assert.fail();
			}

			Map<String, String> properties = new HashMap<>();
			properties.put(Messenger.MIMETYPE_PROPERTY, "application/json");
			Messenger<Person> messenger = new CommonMessenger<>();
			messenger.init(Person.class, new URI("http://admin:admin@localhost:" + socket.getLocalPort()), properties);
			messenger.send(new Person("Peter")).get();

			if (!signal.await(1, TimeUnit.SECONDS))
				Assert.fail();
		}
	}

	@Test
	public void httpFailTest() throws Exception {
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.MIMETYPE_PROPERTY, "application/json");
		Messenger<Person> messenger = new CommonMessenger<>();
		messenger.init(Person.class, new URI("http://admin:admin@localhost:" + 12334), properties);
		try {
			messenger.send(new Person("Peter")).get();
			Assert.fail("Expected TransportConnectionException");
		} catch (TransportConnectionException e) {
			// ignore
		}
	}

	@Test
	public void httpSubTest() throws Exception {
		final CountDownLatch ready = new CountDownLatch(1);
		final CountDownLatch signal = new CountDownLatch(1);
		try (ServerSocket socket = new ServerSocket()) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						socket.bind(null);
						ready.countDown();
						try (Socket s = socket.accept()) {
							try (PrintWriter request = new PrintWriter(s.getOutputStream())) {
								try (BufferedReader response = new BufferedReader(new InputStreamReader(s.getInputStream()))) {
									String responseLine;
									boolean first = true;
									while ((responseLine = response.readLine()) != null && !responseLine.equals("")) {
										if (first) {
											first = false;
											Assert.assertEquals("POST /base HTTP/1.1", responseLine);
										} else {
											int index = responseLine.indexOf(':');
											if (index < 0)
												Assert.fail("Expected HTTP header, but got " + responseLine);
											String key = responseLine.substring(0, index).toLowerCase();
											String value = responseLine.substring(index + 1, responseLine.length()).trim();
											switch (key) {
											case "content-type":
												Assert.assertEquals("application/json", value);
												break;
											case "authorization":
												Assert.assertEquals("Basic YWRtaW46YWRtaW4=", value);
												break;
											default:
												break;
											}
										}
									}

									request.print("HTTP/1.1 200 OK\r\n\r\n");
									request.flush();

									while ((responseLine = response.readLine()) != null) {
										Assert.assertEquals("{\"name\":\"Peter\"}", responseLine);
									}
								}
							}
						}
						signal.countDown();
					} catch (Exception e) {
						e.printStackTrace();
						Assert.fail();
					}
				}
			}).start();
			
			if (!ready.await(100, TimeUnit.MILLISECONDS)) {
				Assert.fail();
			}

			String baseUri = "http://admin:admin@localhost:" + socket.getLocalPort() + "/base";
			Map<String, String> parentProperties = new HashMap<>();
			parentProperties.put(Messenger.MIMETYPE_PROPERTY, "application/json");
			Messenger<Person> parent = new CommonMessenger<>();
			parent.init(Person.class, new URI(baseUri), parentProperties);	
			parent.send(new Person("Peter")).get();
			if (!signal.await(100, TimeUnit.SECONDS))
				Assert.fail();
		}
	}

	@Test
	public void httpValidationTest() throws Exception {
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.MIMETYPE_PROPERTY, "application/json");
		properties.put(Messenger.HTTP_METHOD_PROPERTY, null);
		Messenger<Person> messenger = new CommonMessenger<>();
		try {
			messenger.init(Person.class, new URI("http://localhost"), properties);
			Assert.fail("Expected ValidationException");
		} catch (ValidationException e) {
		}

		properties.put(Messenger.HTTP_METHOD_PROPERTY, "");
		try {
			messenger.init(Person.class, new URI("http://localhost"), properties);
			Assert.fail("Expected ValidationException");
		} catch (ValidationException e) {
		}

		properties.put(Messenger.HTTP_METHOD_PROPERTY, " ");
		try {
			messenger.init(Person.class, new URI("http://localhost"), properties);
			Assert.fail("Expected ValidationException");
		} catch (ValidationException e) {
		}

		properties.put(Messenger.HTTP_METHOD_PROPERTY, "whatever");
		try {
			messenger.init(Person.class, new URI("http://localhost"), properties);
			Assert.fail("Expected ValidationException");
		} catch (ValidationException e) {
		}

		properties.put(Messenger.HTTP_METHOD_PROPERTY, "get");
		messenger.init(Person.class, new URI("http://localhost"), properties);

		properties.put(Messenger.HTTP_METHOD_PROPERTY, "GET");
		messenger.init(Person.class, new URI("http://localhost"), properties);

		properties.put(Messenger.HTTP_METHOD_PROPERTY, "POST");
		messenger.init(Person.class, new URI("http://localhost"), properties);

		properties.put(Messenger.HTTP_METHOD_PROPERTY, "PUT");
		messenger.init(Person.class, new URI("http://localhost"), properties);

		properties.put(Messenger.HTTP_METHOD_PROPERTY, "HEAD");
		messenger.init(Person.class, new URI("http://localhost"), properties);

		properties.put(Messenger.HTTP_METHOD_PROPERTY, "DELETE");
		messenger.init(Person.class, new URI("http://localhost"), properties);

		properties.put(Messenger.HTTP_METHOD_PROPERTY, "OPTIONS");
		messenger.init(Person.class, new URI("http://localhost"), properties);
	}

	@Test
	public void binaryHttpTest() throws Exception {
		final byte[] message = "test".getBytes();
		final CountDownLatch ready = new CountDownLatch(1);
		final CountDownLatch signal = new CountDownLatch(1);
		try (ServerSocket socket = new ServerSocket()) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						socket.bind(null);
						ready.countDown();
						try (Socket s = socket.accept()) {
							try (PrintWriter request = new PrintWriter(s.getOutputStream())) {
								try (BufferedReader response = new BufferedReader(new InputStreamReader(s.getInputStream()))) {
									String responseLine;
									boolean first = true;
									while ((responseLine = response.readLine()) != null && !responseLine.equals("")) {
										if (first) {
											first = false;
											Assert.assertEquals("POST / HTTP/1.1", responseLine);
										} else {
											int index = responseLine.indexOf(':');
											if (index < 0)
												Assert.fail("Expected HTTP header, but got " + responseLine);
											String key = responseLine.substring(0, index).toLowerCase();
											String value = responseLine.substring(index + 1, responseLine.length()).trim();
											switch (key) {
											case "content-type":
												Assert.assertEquals("application/octet-stream", value);
												break;
											default:
												break;
											}
										}
									}

									request.print("HTTP/1.1 200 OK\r\n\r\n");
									request.flush();

									while ((responseLine = response.readLine()) != null) {
										Assert.assertEquals("test", responseLine);
									}
								}
							}
						}
						signal.countDown();
					} catch (Exception e) {
						e.printStackTrace();
						Assert.fail();
					}
				}
			}).start();

			if (!ready.await(100, TimeUnit.MILLISECONDS)) {
				Assert.fail();
			}

			Map<String, String> properties = new HashMap<>();
			properties.put(Messenger.MIMETYPE_PROPERTY, Messenger.BINARY_MIMETYPE);
			Messenger<byte[]> messenger = new CommonMessenger<>();
			messenger.init(byte[].class, new URI("http://localhost:" + socket.getLocalPort()), properties);
			messenger.send(message).get();

			if (!signal.await(1, TimeUnit.SECONDS))
				Assert.fail();
		}
	}

	// @Test
	public void jdbcTest() throws Exception {
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.JDBC_TABLE_NAME_PROPERTY, "ec");
		properties.put(Messenger.DATA_CONVERTER_AVOID_DUPLICATES_PROPERTY, Boolean.TRUE.toString());
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY,
				"specName, creationDate, ((reportName, (groupName, (((value as epcValue in epc) in member) in groupList) in group) in report) in reports)");
		// properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY,
		// "specName, creationDate, (((groupName, (((value as epcValue in epc),
		// (((profile, count? as tagCount, firstSightingTime? as sightingFirst,
		// lastSightingTime? as sightingLast, ((readerName as sightingReader,
		// ((host as sightingHost, antenna as sightingAntenna, strength as
		// sightingStrength in sighting) in sightings?) in statBlock) in
		// statBlocks?) in stat) in stats) in extension) in member) in
		// groupList) in group) in report) in reports)");
		// properties
		// .put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY,
		// "specName, creationDate, (((groupName, (((value as epcValue in epc),
		// (((profile as profile1, firstSightingTime as sightingFirst,
		// lastSightingTime as sightingLast in stat[profile=TagTimestamps]?),
		// (profile as profile2, count as tagCount in stat[profile=TagCount]?),
		// (profile as profile3, ((readerName as reader1 in statBlock[0]),
		// (readerName as reader2 in statBlock[1]) in statBlocks) in
		// stat[profile=ReaderNames]?) in stats) in extension) in member) in
		// groupList) in group) in report) in reports)");

		Messenger<ECReports> messenger = new CommonMessenger<>();
		messenger.init(ECReports.class, new URI("jdbc:mysql://10.65.54.165:3306/test?user=root&password=root"), properties);
		messenger.send(PlainDataConverterTest.createECReports()).get();
		messenger.dispose();
	}
}