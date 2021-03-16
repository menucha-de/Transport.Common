package havis.transport.common;

import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Environment {

	private final static Logger log = Logger.getLogger(Environment.class.getName());
	private final static Properties properties = new Properties();

	static {
		try (InputStream stream = Environment.class.getClassLoader().getResourceAsStream("havis.transport.properties")) {
			properties.load(stream);
		} catch (Exception e) {
			log.log(Level.SEVERE, "Failed to load environment properties", e);
		}
	}

	public static final String CERT_FOLDER = properties.getProperty("havis.transport.certFolder", "conf/havis/transport/certs");
	public static final String TRUST_FILE_NAME = properties.getProperty("havis.transport.trustFileName", "ca");
	public static final String KEYSTORE_FILE_NAME = properties.getProperty("havis.transport.keyStoreFileName", "key");
	//TODO MICA depended KeyStore Password
	public static final String KEYSTORE_PASSWD = properties.getProperty("havis.transport.keyStorePasswd", "passwd");

	public static final String SUBSCRIBER_CONFIG = properties.getProperty("havis.transport.config.subscriber", "conf/havis/transport/subscribers.json");
}
