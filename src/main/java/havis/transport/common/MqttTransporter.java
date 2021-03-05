package havis.transport.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.SocketFactory;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import havis.transport.Marshaller;
import havis.transport.Messenger;
import havis.transport.TransportConnectionException;
import havis.transport.TransportException;
import havis.transport.Transporter;
import havis.transport.ValidationException;

/**
 * MQTT transporter
 * 
 * @param <T>
 *            type of messages
 */
public class MqttTransporter<T> extends StreamTransporter<T> {

	private final static Logger log = Logger.getLogger(MqttTransporter.class.getName());

	private final static String CLIENT_ID_PARAMETER = "clientid";
	private final static String QOS_PARAMETER = "qos";

	private final static int DEFAULT_PORT = 1883;
	private final static int DEFAULT_TIMEOUT = 1000;

	private URI uri;

	private String topic;
	private Integer qos;
	private boolean isConnectionLost = false;

	private MqttClient client = null;
	private MqttConnectOptions connectOptions = new MqttConnectOptions();

	private Map<String, StreamCallback> subscriptions = new ConcurrentHashMap<String, StreamCallback>();

	@Override
	protected String getDefaultMimeType() {
		return "application/json";
	}

	@Override
	protected void init(URI uri, Map<String, String> properties) throws ValidationException {
		if (uri == null)
			throw new ValidationException("URI must not be null");
		this.uri = uri;
		if (uri.getPath() == null)
			throw new ValidationException("MQTT topic must be specified using the path of the URI");
		topic = uri.getPath();
		// Checks for subscriber special chars
		if (topic.contains("#") || topic.contains("+")) {
			throw new ValidationException("MQTT topic should not contain '#' or '+'");
		}
		topic = removeLeadingSlash(topic);
		String userInfo = uri.getUserInfo();
		if (userInfo != null && userInfo.length() > 0) {
			String[] auth = userInfo.split(":");
			if (auth.length > 0) {
				connectOptions.setUserName(auth[0]);
				if (auth.length > 1) {
					connectOptions.setPassword(auth[1].toCharArray());
				}
			}
		}
		int timeout = DEFAULT_TIMEOUT;
		if (properties != null) {
			for (Entry<String, String> entry : properties.entrySet()) {
				String key = entry.getKey();
				if (key != null && key.startsWith(Transporter.PREFIX + "MQTT")) {
					switch (key) {
					case Messenger.MQTT_TIMEOUT_PROPERTY:
						try {
							timeout = Integer.parseInt(entry.getValue());
							if (timeout < 0)
								throw new ValidationException("Invalid timeout value '" + entry.getValue() + "'");
						} catch (NumberFormatException e) {
							throw new ValidationException("Invalid timeout value '" + entry.getValue() + "'", e);
						}
						break;
					default:
						throw new ValidationException("Unknown property key '" + key + "'");
					}
				}
			}
		}

		Map<String, String> queryParameters = Utils.getQueryParameters(uri);
		if (!queryParameters.containsKey(CLIENT_ID_PARAMETER))
			throw new ValidationException("'" + CLIENT_ID_PARAMETER + "' must be set as URI query parameter for MQTT transporter");

		if (queryParameters.containsKey(QOS_PARAMETER)) {
			try {
				qos = Integer.valueOf(queryParameters.get(QOS_PARAMETER));
			} catch (NumberFormatException e) {
				throw new ValidationException("Invalid MQTT qos value '" + queryParameters.get(QOS_PARAMETER) + "'", e);
			}
		}

		connectOptions.setConnectionTimeout(Math.max((int) Math.round(timeout / 1000.0), 1));
		connectOptions.setAutomaticReconnect(true);

		if (uri.getHost() == null) {
			throw new ValidationException("No MQTT host specified");
		}

		Logger.getLogger("org.eclipse.paho.client.mqttv3.internal.ClientState").setLevel(Level.OFF);

		String mqttUri = ("mqtts".equals(uri.getScheme()) ? "ssl" : "tcp") + "://" + this.uri.getHost() + ":"
				+ (this.uri.getPort() < 0 ? DEFAULT_PORT : this.uri.getPort());
		try {
			client = new MqttClient(mqttUri, queryParameters.get(CLIENT_ID_PARAMETER), new MemoryPersistence());
			client.setCallback(new MqttCallbackExtended() {

				@Override
				public void messageArrived(String topic, MqttMessage message) throws Exception {
				}

				@Override
				public void deliveryComplete(IMqttDeliveryToken token) {
				}

				@Override
				public void connectionLost(Throwable cause) {
					// TODO Monitoring
					isConnectionLost = true;
					log.log(Level.WARNING, "Connection lost to " + client.getCurrentServerURI() + ": " + (cause != null ? cause.toString() : "Unknown error"), cause);
				}

				@Override
				public void connectComplete(boolean reconnect, String serverUri) {
					isConnectionLost = false;
					if (reconnect) {
						// TODO Monitoring
						log.log(Level.INFO, "Re-connected to server {0}", serverUri);
						for (Entry<String, StreamCallback> s : subscriptions.entrySet()) {
							try {
								String path = removeLeadingSlash(s.getKey());
								client.unsubscribe(path);
								subscribeInternal(s.getKey(), s.getValue());
							} catch (MqttException e) {
								log.log(Level.SEVERE, "Re-Subscribe on topic " + s.getKey() + " failed: " + e.getMessage(), e);
							}
						}
					}
				}

			});
		} catch (MqttException e) {
			throw new ValidationException("Failed to create MQTT client for URI '" + uri + "'", e);
		}
	}

	@Override
	public boolean supportsSocketFactory() {
		return true;
	}

	@Override
	public void setSocketFactory(SocketFactory socketFactory) throws TransportException {
		try {
			if (socketFactory != null) {
				connectOptions.setSocketFactory(socketFactory);
				if (client.isConnected()) {
					client.disconnect();
					client.connect(connectOptions);
				}
			}
		} catch (Exception e) {
			throw new TransportException("Could not update MQTT certificate handling: " + e.getMessage(), e);
		}
	}

	@Override
	protected synchronized void send(Marshaller<T> marshaller, T message, String name, String path, Map<String, String> properties) throws TransportException {
		send(path, marshaller, message);
	}

	@Override
	protected synchronized void send(Marshaller<T> marshaller, T message) throws TransportException {
		send(topic, marshaller, message);
	}

	private synchronized void send(String topic, Marshaller<T> marshaller, T message) throws TransportException {
		if(isConnectionLost) {
			throw new TransportConnectionException("Connection is currently recovering");
		}
		if (client == null)
			throw new TransportException("MQTT client not initialized");
		topic = removeLeadingSlash(topic);
		try {
			if (!client.isConnected())
				try {
					client.connect(connectOptions);
				} catch (MqttException e) {
					switch (e.getReasonCode()) {
					case MqttException.REASON_CODE_SERVER_CONNECT_ERROR:
						throw new TransportConnectionException("Failed to connect to MQTT URI '" + uri + "': " + e.getMessage(), e);
					default:
						throw e;
					}
				}
			try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
				marshaller.marshal(message, stream);
				byte[] bytes = stream.toByteArray();
				if (qos != null)
					client.publish(topic, bytes, qos.intValue(), false);
				else
					client.publish(topic, new MqttMessage(bytes));
			}
		} catch (TransportConnectionException e) {
			throw e;
		} catch (Exception e) {
			throw new TransportException("Failed to send MQTT message: " + e.getMessage(), e);
		}
	}

	@Override
	protected synchronized void subscribe(String topic, final StreamCallback callback) throws TransportException {
		if (client == null)
			throw new TransportException("MQTT client not initialized");

		if (topic == null)
			throw new TransportException("Either topic or topic extension must be provided for MQTT");

		

		try {
			if (!client.isConnected())
				client.connect(connectOptions);
			subscribeInternal(topic, callback);
			subscriptions.put(topic, callback);
		} catch (Exception e) {
			throw new TransportException("Failed to subscribe to MQTT topic: " + e.getMessage(), e);
		}
		
	}
	
	private void subscribeInternal(String topic, final StreamCallback callback) throws MqttException {
		final String originalPath = topic;
		final String path = removeLeadingSlash(topic);
		client.subscribe(path, new IMqttMessageListener() {
			@Override
			public void messageArrived(String topic, MqttMessage message) throws Exception {
				callback.arrived(originalPath, new ByteArrayInputStream(message.getPayload()));
			}
		});
	}

	@Override
	protected synchronized void unsubscribe(String path) throws TransportException {
		if (client == null)
			throw new TransportException("MQTT client not initialized");
		String path1 = removeLeadingSlash(path);
		try {
			if (!client.isConnected())
				client.connect(connectOptions);
			client.unsubscribe(path1);
			subscriptions.remove(path);
		} catch (Exception e) {
			throw new TransportException("Failed to unsubscribe from MQTT topic: " + e.getMessage(), e);
		}
	}

	@Override
	public void dispose() {
		if (client != null) {
			try {
				try {
					if (client.isConnected()){
						client.setCallback(null);
						client.disconnect();
					}
				} finally {
					client.close();
				}
			} catch (NullPointerException e) {
				// ignore any exceptions (sometimes an NPE is thrown by the MQTT
				// client)
				log.log(Level.FINE, "Failed to close MQTT connection", e);
			} catch (Exception e) {
				log.log(Level.SEVERE, "Failed to close MQTT connection: " + e.getMessage(), e);
			} finally {
				client = null;
			}
		}
	}

	private String removeLeadingSlash(String topic) {
		if (topic != null && topic.startsWith("/")) {
			topic = topic.substring(1);
		}
		return topic;
	}
}
