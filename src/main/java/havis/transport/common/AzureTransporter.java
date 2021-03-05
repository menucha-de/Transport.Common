package havis.transport.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.azure.sdk.iot.device.DeviceClient;
import com.microsoft.azure.sdk.iot.device.IotHubClientProtocol;
import com.microsoft.azure.sdk.iot.device.IotHubConnectionStatusChangeCallback;
import com.microsoft.azure.sdk.iot.device.IotHubConnectionStatusChangeReason;
import com.microsoft.azure.sdk.iot.device.IotHubEventCallback;
import com.microsoft.azure.sdk.iot.device.IotHubMessageResult;
import com.microsoft.azure.sdk.iot.device.IotHubStatusCode;
import com.microsoft.azure.sdk.iot.device.Message;
import com.microsoft.azure.sdk.iot.device.MessageCallback;
import com.microsoft.azure.sdk.iot.device.transport.IotHubConnectionStatus;

import havis.transport.Marshaller;
import havis.transport.Messenger;
import havis.transport.TransportConnectionException;
import havis.transport.TransportException;
import havis.transport.Transporter;
import havis.transport.ValidationException;

class AzureTransporter<T> extends StreamTransporter<T> {

	private final static Logger log = Logger.getLogger(AzureTransporter.class.getName());

	private long timeout = 1000;
	private long sasTokenExpiryTime = 2400;
	private IotHubClientProtocol protocol = IotHubClientProtocol.MQTT;
	private String pathToCertificate = null;
	private String connectionString;
	private DeviceClient client;
	private StreamCallback callback;

	private MessageCallback messageCallback = new MessageCallback() {
		@Override
		public IotHubMessageResult execute(Message message, Object context) {
			if (callback != null) {
				callback.arrived(null, new ByteArrayInputStream(message.getBytes()));
				return IotHubMessageResult.COMPLETE;
			} else {
				return IotHubMessageResult.REJECT;
			}
		}
	};

	private IotHubConnectionStatusChangeCallback statusChangeCallback = new IotHubConnectionStatusChangeCallback() {
		@Override
		public void execute(IotHubConnectionStatus status, IotHubConnectionStatusChangeReason reason, Throwable t, Object o) {
			if (status == IotHubConnectionStatus.DISCONNECTED) {
				log.info("Attempting to close azure client connection because of unexpected disconnect" + (t != null ? (": " + t.toString()) : ""));
				// workaround, see:
				// https://github.com/Azure/azure-iot-sdk-java/issues/603#issuecomment-600896578
				try {
					client.closeNow();
				} catch (IOException e) {
					// ignore
				}
				try {
					client = createClient();
					log.info("Successfully closed azure client connection because of unexpected disconnect" + (t != null ? (": " + t.toString()) : ""));
				} catch (ValidationException e) {
					log.log(Level.SEVERE, "Failed to recreate azure client connection: " + e.toString(), e);
				}
			}
		}
	};

	@Override
	protected String getDefaultMimeType() {
		return "application/json";
	}

	@Override
	protected void init(URI uri, Map<String, String> properties) throws ValidationException {
		if (uri == null)
			throw new ValidationException("URI must not be null");
		boolean connectDirectly = true;
		this.connectionString = uri.getSchemeSpecificPart();
		while (this.connectionString.length() > 0 && this.connectionString.charAt(0) == '/')
			this.connectionString = this.connectionString.substring(1);
		if (properties != null) {
			for (Entry<String, String> entry : properties.entrySet()) {
				String key = entry.getKey();
				if (key != null && key.startsWith(Transporter.PREFIX + "Azure")) {
					switch (key) {
					case Messenger.AZURE_TIMEOUT_PROPERTY:
						try {
							timeout = Integer.parseInt(entry.getValue());
							if (timeout < 0)
								throw new ValidationException("Invalid timeout value '" + entry.getValue() + "'");
						} catch (NumberFormatException e) {
							throw new ValidationException("Invalid timeout value '" + entry.getValue() + "'", e);
						}
						break;
					case Messenger.AZURE_ONDEMAND_PROPERTY:
						try {
							connectDirectly = !Boolean.parseBoolean(entry.getValue()); // log.info("Transport failed: " + e.toString());
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

		client = createClient();

		if (connectDirectly) {
			try {
				client.open();
			} catch (IOException e) {
				throw new ValidationException("Failed to establish connection: " + e.getMessage(), e);
			}
		}
	}

	private DeviceClient createClient() throws ValidationException {
		Logger.getLogger("com.microsoft.azure.sdk.iot.device.transport.IotHubTransport").setLevel(Level.OFF);
		Logger.getLogger("com.microsoft.azure.sdk.iot.device.transport.mqtt.MqttIotHubConnection").setLevel(Level.OFF);
		final DeviceClient c;
		try {
			c = new DeviceClient(this.connectionString, this.protocol);
		} catch (URISyntaxException e) {
			throw new ValidationException("Failed to initialize azure client: " + e.getMessage(), e);
		}
		if (pathToCertificate != null) {
			c.setOption("SetCertificatePath", this.pathToCertificate);
		}
		c.setOption("SetSASTokenExpiryTime", this.sasTokenExpiryTime);
		c.registerConnectionStatusChangeCallback(statusChangeCallback, null);
		c.setMessageCallback(messageCallback, null);
		return c;
	}

	@Override
	protected void send(Marshaller<T> marshaller, T message) throws TransportException {
		try {
			final AtomicReference<IotHubStatusCode> messageStatus = new AtomicReference<IotHubStatusCode>(IotHubStatusCode.MESSAGE_EXPIRED);
			final CountDownLatch latch = new CountDownLatch(1);
			client.open();

			ByteArrayOutputStream data = new ByteArrayOutputStream();
			marshaller.marshal(message, data);
			Message msg = new Message(data.toByteArray());
			msg.setMessageId(UUID.randomUUID().toString());
			msg.setExpiryTime(this.timeout);
			msg.setContentEncoding("UTF-8");
			msg.setContentTypeFinal(getDefaultMimeType());

			client.sendEventAsync(msg, new IotHubEventCallback() {
				@Override
				public void execute(IotHubStatusCode status, Object context) {
					messageStatus.set(status);
					latch.countDown();
				}
			}, null);

			try {
				latch.await(this.timeout * 2, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				// was cancelled
			}

			if (messageStatus.get() != IotHubStatusCode.OK && messageStatus.get() != IotHubStatusCode.OK_EMPTY) {
				throw new TransportConnectionException("Azure transport failed: Message status was " + messageStatus.get().name());
			} else {
				log.fine("Azure message was sent: " + messageStatus.get().name());
			}
		} catch (TransportException e) {
			throw e;
		} catch (IOException e) {
			throw new TransportConnectionException("Azure transport failed: " + e.getMessage(), e);
		} catch (Throwable e) {
			throw new TransportException("Azure transport failed: " + e.toString(), e);
		}
	}

	@Override
	protected void subscribe(String path, StreamCallback callback) throws TransportException {
		if (path == null || path.length() != 0) {
			this.callback = callback;
		} else {
			throw new TransportException("Azure transport does not support paths");
		}
	}

	@Override
	protected void unsubscribe(String path) throws TransportException {
		callback = null;
	}

	@Override
	public void dispose() {
		if (client != null) {
			try {
				client.closeNow();
			} catch (IOException e) {
				log.log(Level.SEVERE, "Failed to close azure client connection", e);
			} finally {
				client = null;
			}
		}
	}
}
