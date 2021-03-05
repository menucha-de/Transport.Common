package havis.transport.common;

import havis.transport.Messenger;
import havis.transport.Transporter;
import havis.transport.ValidationException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

/**
 * Utility methods for transport
 */
public class Utils {

	private final static Logger log = Logger.getLogger(Messenger.class.getName());

	private final static Map<String, Class<?>> transporters = new LinkedHashMap<>();
	static {
		transporters.put("http", HttpTransporter.class);
		transporters.put("https", HttpTransporter.class);
		transporters.put("mqtt", MqttTransporter.class);
		transporters.put("mqtts", MqttTransporter.class);
		transporters.put("tcp", TcpTransporter.class);
		transporters.put("udp", UdpTransporter.class);
		transporters.put("jdbc", JdbcTransporter.class);
		transporters.put("azure", AzureTransporter.class);
	}

	private Utils() {
	}

	/**
	 * Retrieve the query parameters as a {@link Map} from the specified
	 * {@link URI}
	 * 
	 * @param uri
	 *            the URI
	 * @return the query parameters
	 */
	public static Map<String, String> getQueryParameters(URI uri) {
		try {
			Map<String, String> result = new LinkedHashMap<String, String>();
			if (uri.getRawQuery() == null)
				return result;

			String[] params = uri.getRawQuery().split("&");
			for (String param : params) {
				String[] pair = URLDecoder.decode(param, StandardCharsets.UTF_8.name()).split("=");
				result.put(pair[0], pair.length > 1 ? pair[1] : null);
			}
			return result;
		} catch (UnsupportedEncodingException e) {
			throw new IllegalStateException(e);
		}
	}

	public static Map<String, Class<?>> getCommonTransporters() {
		return transporters;
	}

	/**
	 * @return the available subscriber types or URI schemes
	 */
	public static List<String> getAvailableTransporters() {
		try {
			return Connector.getFactory().getTypes(Transporter.class);
		} catch (ValidationException e) {
			log.severe("Failed to retrieve list of available transporters: " + e.getMessage());
			return Collections.emptyList();
		}
	}

	/**
	 * Decrypt key file using the specified pass phrase and AES algorithm
	 * 
	 * @param passphrase
	 *            the pass phrase
	 * @param input
	 *            the input stream
	 * @return the input stream on the decrypted key file
	 * @throws ValidationException
	 *             if decryption failed
	 */
	public static InputStream decryptKeyFile(String passphrase, InputStream input) throws ValidationException {
		try {
			Key secretKey = new SecretKeySpec(passphrase.getBytes(), "AES");
			Cipher cipher = Cipher.getInstance("AES");
			cipher.init(Cipher.DECRYPT_MODE, secretKey);

			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			int nRead;
			byte[] data = new byte[16384];
			while ((nRead = input.read(data, 0, data.length)) != -1) {
				buffer.write(data, 0, nRead);
			}
			byte[] outputBytes = cipher.doFinal(buffer.toByteArray());
			return new ByteArrayInputStream(outputBytes);
		} catch (Exception e) {
			throw new ValidationException("Failed to decrypt key file: " + e.getMessage(), e);
		} finally {
			if (input != null)
				try {
					input.close();
				} catch (IOException e) {
					throw new ValidationException("Failed to close input stream", e);
				}
		}
	}

	static <T> Transporter<T> getTransporter(String transporter) throws ValidationException {
		if (transporter != null) {
			switch (transporter) {
			case "http":
			case "https":
				return new HttpTransporter<T>();
			case "mqtt":
			case "mqtts":
				return new MqttTransporter<T>();
			case "tcp":
				return new TcpTransporter<T>();
			case "udp":
				return new UdpTransporter<T>();
			case "jdbc":
				return new JdbcTransporter<T>();
			case "azure":
				return new AzureTransporter<T>();
			default:
				@SuppressWarnings("unchecked")
				Transporter<T> instance = Connector.getFactory().newInstance(Transporter.class, transporter);
				return instance;
			}
		}
		return null;
	}
}
