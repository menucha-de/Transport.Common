package havis.transport.common;

import havis.transport.Marshaller;
import havis.transport.Messenger;
import havis.transport.TransportException;
import havis.transport.Transporter;
import havis.transport.ValidationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Map.Entry;

class UdpTransporter<T> extends StreamTransporter<T> {

	private DatagramSocket socket;
	private InetAddress address;
	private int port;

	@Override
	protected String getDefaultMimeType() {
		return Messenger.DEFAULT_MIMETYPE;
	}

	@Override
	protected void init(URI uri, Map<String, String> properties) throws ValidationException {
		if (uri == null)
			throw new ValidationException("URI must not be null");
		if (properties != null) {
			for (Entry<String, String> entry : properties.entrySet()) {
				String key = entry.getKey();
				if (key != null && key.startsWith(Transporter.PREFIX + "UDP")) {
					switch (key) {
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
		port = uri.getPort();

		try {
			address = InetAddress.getByName(uri.getHost());
		} catch (UnknownHostException e) {
			throw new ValidationException("Unknown host '" + uri.getHost() + "': " + e.getMessage());
		}

		try {
			socket = new DatagramSocket();
		} catch (SocketException e) {
			throw new ValidationException("Failed to create datagram socket " + e.getMessage());
		}
	}

	@Override
	protected void send(Marshaller<T> marshaller, T message) throws TransportException {
		try {
			try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
				marshaller.marshal(message,stream);
				byte[] bytes = stream.toByteArray();
				socket.send(new DatagramPacket(bytes, bytes.length, address, port));
			}
		} catch (IOException e) {
			throw new TransportException("UDP transport failed: " + e.getMessage(), e);
		}
	}

	@Override
	public void dispose() {
	}
}