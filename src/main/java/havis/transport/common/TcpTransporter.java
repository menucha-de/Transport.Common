package havis.transport.common;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.util.Map;
import java.util.Map.Entry;

import havis.transport.Marshaller;
import havis.transport.Messenger;
import havis.transport.TransportConnectionException;
import havis.transport.TransportException;
import havis.transport.Transporter;
import havis.transport.ValidationException;

class TcpTransporter<T> extends StreamTransporter<T> {

	private int timeout = 1000;
	private URI uri;

	@Override
	protected String getDefaultMimeType() {
		return Messenger.DEFAULT_MIMETYPE;
	}

	@Override
	protected void init(URI uri, Map<String, String> properties) throws ValidationException {
		if (uri == null)
			throw new ValidationException("URI must not be null");
		this.uri = uri;
		if (properties != null) {
			for (Entry<String, String> entry : properties.entrySet()) {
				String key = entry.getKey();
				if (key != null && key.startsWith(Transporter.PREFIX + "TCP")) {
					switch (key) {
					case Messenger.TCP_TIMEOUT_PROPERTY:
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

		if (uri.getHost() == null) {
			throw new ValidationException("No host specified");
		}
		if (uri.getPort() == -1) {
			throw new ValidationException("No port specified");
		}
	}

	@Override
	protected void send(Marshaller<T> marshaller, T message) throws TransportException {
		try {
			Socket client = new Socket();
			client.connect(new InetSocketAddress(uri.getHost(), uri.getPort()), timeout);
			try {
				marshaller.marshal(message, client.getOutputStream());
			} finally {
				client.close();
			}
		} catch (IOException e) {
			throw new TransportConnectionException("Failed to connect to URI '" + uri + "'", e);
		} catch (Exception e) {
			throw new TransportException("TCP transport failed: " + e.getMessage(), e);
		}
	}

	@Override
	public void dispose() {
	}
}