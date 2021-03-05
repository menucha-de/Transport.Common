package havis.transport.common;

import havis.transport.Marshaller;
import havis.transport.Messenger;
import havis.transport.TransportConnectionException;
import havis.transport.TransportException;
import havis.transport.Transporter;
import havis.transport.ValidationException;

import java.io.IOException;
import java.net.CookieManager;
import java.net.HttpCookie;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.net.SocketFactory;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import javax.ws.rs.HttpMethod;
import javax.xml.bind.DatatypeConverter;

class HttpTransporter<T> extends StreamTransporter<T> {

	private URI uri;
	private int timeout = 1000;
	private String method = "POST";
	private boolean bypassSslVerification = false;
	private String mimeType = Messenger.DEFAULT_MIMETYPE;

	private ExecutorService executor = Executors.newSingleThreadExecutor();
	private CookieManager cookieManager = new CookieManager();

	private SSLSocketFactory current = null;

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
				if (key != null && key.startsWith(Transporter.PREFIX + "HTTP")) {
					switch (key) {
					case Messenger.HTTP_TIMEOUT_PROPERTY:
						try {
							timeout = Integer.parseInt(entry.getValue());
							if (timeout < 0)
								throw new ValidationException("Invalid timeout value '" + entry.getValue() + "'");
						} catch (NumberFormatException e) {
							throw new ValidationException("Invalid timeout value '" + entry.getValue() + "'", e);
						}
						break;
					case Messenger.HTTP_METHOD_PROPERTY:
						method = entry.getValue();
						if (method == null || (method = method.trim().toUpperCase()).isEmpty())
							throw new ValidationException("HTTP method not specified");
						switch (method) {
						case HttpMethod.DELETE:
						case HttpMethod.GET:
						case HttpMethod.HEAD:
						case HttpMethod.OPTIONS:
						case HttpMethod.POST:
						case HttpMethod.PUT:
							break;
						default:
							throw new ValidationException("Invalid HTTP method value '" + entry.getValue() + "'");
						}
						break;
					case Messenger.HTTPS_BYPASS_SSL_VERIFICATION_PROPERTY:
						bypassSslVerification = Boolean.TRUE.toString().equalsIgnoreCase(entry.getValue());
						break;
					default:
						throw new ValidationException("Unknown property key '" + key + "'");
					}
				} else if (Messenger.MIMETYPE_PROPERTY.equals(key)) {
					mimeType = entry.getValue();
				}
			}
		}

		if (uri.getHost() == null) {
			throw new ValidationException("No host specified");
		}
	}

	@Override
	public boolean supportsSocketFactory() {
		return true;
	}

	@Override
	public void setSocketFactory(SocketFactory socketFactory) throws TransportException {
		try {
			current = (SSLSocketFactory) socketFactory;
		} catch (Exception e) {
			throw new TransportException("Could not update certificate handling", e);
		}
	}

	@Override
	protected void send(Marshaller<T> marshaller, T message, String name, String path, Map<String, String> properties) throws TransportException {
		if (properties != null) {
			for (Entry<String, String> entry : properties.entrySet()) {
				String key = entry.getKey();
				if (key != null && key.startsWith(Transporter.PREFIX + "HTTP")) {
					throw new TransportException("Property '" + key + "' cannot be changed during transport");
				}
			}
		}

		URI extendedUri;
		try {
			extendedUri = new URI(this.uri.getScheme(), this.uri.getUserInfo(), this.uri.getHost(), this.uri.getPort(), path, this.uri.getQuery(),
					this.uri.getFragment());
		} catch (URISyntaxException e) {
			throw new TransportException("Failed to extend URI path: " + e.getMessage(), e);
		}
		send(extendedUri, marshaller, message);
	}

	@Override
	protected void send(final Marshaller<T> marshaller, T message) throws TransportException {
		send(this.uri, marshaller, message);
	}

	private void send(URI uri, final Marshaller<T> marshaller, final T message) throws TransportException {
		try {
			final HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();

			SSLSocketFactory trustAllContext = SSLContextManager.createTrustedAllContext();
			HostnameVerifier trustAllVerifier = SSLContextManager.createTrustAllVerifier();
			if (bypassSslVerification && connection instanceof HttpsURLConnection && trustAllContext != null && trustAllVerifier != null) {
				((HttpsURLConnection) connection).setSSLSocketFactory(trustAllContext);
				((HttpsURLConnection) connection).setHostnameVerifier(trustAllVerifier);
			} else {
				if (current != null && connection instanceof HttpsURLConnection) {
					((HttpsURLConnection) connection).setSSLSocketFactory(current);
				}
			}

			connection.setConnectTimeout(timeout);
			connection.setReadTimeout(timeout);
			connection.setDoOutput(true);
			connection.setRequestMethod(method);
			connection.setRequestProperty("Content-type", mimeType);

			if (cookieManager.getCookieStore().getCookies().size() > 0) {
				// While joining the Cookies, use ',' or ';' as needed,
				// most servers are using ';'
				StringBuilder value = new StringBuilder();
				for (HttpCookie cookie : cookieManager.getCookieStore().getCookies()) {
					if (value.length() > 0)
						value.append(';');
					// always use simple format
					value.append(cookie.getName() + "=" + cookie.getValue());
				}
				connection.setRequestProperty("Cookie", value.toString());
			}

			if (uri.getUserInfo() != null) {
				String basicAuth = "Basic " + DatatypeConverter.printBase64Binary(uri.getUserInfo().getBytes());
				connection.setRequestProperty("Authorization", basicAuth);
			}

			// Since the OpenJDK SocketInputStream.read0 method might hang and
			// not respond to Thread.interrupt calls, we have to wrap the
			// connection handling into a separate thread. If waiting on the
			// thread is interrupted, we try canceling the socket operation,
			// this might fail and the thread will hang until the socket timeout
			// is reached.
			Future<Void> task = executor.submit(new Callable<Void>() {

				@Override
				public Void call() throws Exception {
					connection.connect();
					try {
						marshaller.marshal(message, connection.getOutputStream());
						int code = connection.getResponseCode();
						if (code < HttpURLConnection.HTTP_OK || code >= HttpURLConnection.HTTP_OK + 100) {
							throw new IOException("HTTP " + code + ": " + connection.getResponseMessage());
						}
						for (Entry<String, List<String>> entry : connection.getHeaderFields().entrySet()) {
							if ("Set-Cookie".equalsIgnoreCase(entry.getKey()) && entry.getValue() != null) {
								for (String cookie : entry.getValue()) {
									cookieManager.getCookieStore().add(null, HttpCookie.parse(cookie).get(0));
								}
							}
						}
					} catch (Exception e) {
						throw e;
					} finally {
						connection.disconnect();
					}
					return null;
				}
			});

			// wait for the connection handling to finish
			try {
				task.get();
			} catch (InterruptedException e) {
				// waiting was interrupted, cancel
				task.cancel(true);
				try {
					connection.disconnect();
				} catch (Exception ex) {
					// ignore
				}
				Thread.currentThread().interrupt();
			}
		} catch (IOException e) {
			throw new TransportException("HTTP transport failed: " + e.getMessage(), e);
		} catch (ExecutionException e) {
			if (e.getCause() instanceof IOException)
				throw new TransportConnectionException("HTTP transport failed: " + e.getMessage(), e);
			throw new TransportException("HTTP transport failed: " + e.getMessage(), e);
		}
	}

	@Override
	public void dispose() {
		executor.shutdownNow();
	}
}