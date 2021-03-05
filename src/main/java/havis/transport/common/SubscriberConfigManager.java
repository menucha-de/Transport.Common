package havis.transport.common;

import havis.transport.ValidationException;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

public class SubscriberConfigManager {

	private final static Logger LOG = Logger.getLogger(SubscriberConfigManager.class.getName());

	private final static ObjectMapper MAPPER = new ObjectMapper();

	private static SubscriberConfiguration instance;

	public synchronized static SubscriberConfiguration getInstance() {
		if (instance == null) {
			try {
				instance = deserialize();
				return instance;
			} catch (ValidationException e) {
				LOG.log(Level.SEVERE, "Failed to deserialize Cycle config.", e);
				return instance = new SubscriberConfiguration();
			}
		} else {
			return instance;
		}
	}

	private SubscriberConfigManager() {
	}

	/**
	 * Serializes the subscriber config
	 * 
	 * @throws ValidationException
	 *             If serialization failed.
	 */
	public static synchronized void serialize() throws ValidationException {
		File tmpFile = null;
		try {
			String filename = Environment.SUBSCRIBER_CONFIG;
			File destination = new File(filename);
			File parent = destination.getParentFile();
			if (parent != null) {
				// create parent directory
				if (!parent.mkdirs() && !parent.exists()) {
					LOG.warning("Failed to create parent directory '" + parent.getAbsolutePath() + "'.");
				}
			}

			// creation of temporary backup file
			tmpFile = File.createTempFile(filename, ".bak", parent);
			LOG.fine("Created temporary file '" + tmpFile.getAbsolutePath() + "'.");

			// writing configuration to temporary backup file
			MAPPER.writerWithDefaultPrettyPrinter().writeValue(tmpFile, instance);

			// Replacing deprecated configuration by new configuration file
			if (tmpFile.renameTo(destination)) {
				LOG.fine("Replaced configuration file.");
			} else {
				throw new Exception("Replacing " + destination.getAbsolutePath() + " with '" + tmpFile.getAbsolutePath() + "' failed.");
			}
		} catch (Exception e) {
			LOG.log(Level.SEVERE, "Failed to persist config", e);
			// delete temporary file
			if (tmpFile != null && tmpFile.exists()) {
				tmpFile.delete();
			}
			throw new ValidationException("Failed to persist config", e);
		}
	}

	private static synchronized SubscriberConfiguration deserialize() throws ValidationException {
		String filename = Environment.SUBSCRIBER_CONFIG;
		File configFile = new File(filename);
		if (configFile.exists()) {
			try {
				SubscriberConfiguration config = MAPPER.readValue(configFile, SubscriberConfiguration.class);
				return config;
			} catch (Exception e) {
				throw new ValidationException("Failed to load config", e);
			}
		} else {
			LOG.fine("Config '" + filename + "' does not exist.");
		}
		return new SubscriberConfiguration();
	}

}
