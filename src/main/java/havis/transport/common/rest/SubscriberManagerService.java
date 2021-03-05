package havis.transport.common.rest;

import havis.net.rest.shared.Resource;
import havis.transport.Subscriber;
import havis.transport.SubscriberManager;
import havis.transport.ValidationException;
import havis.transport.common.Environment;
import havis.transport.common.SubscriberConfigManager;
import havis.transport.common.SubscriberConfiguration;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.KeyStore;
import java.security.Key;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Enumeration;

import javax.annotation.security.PermitAll;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

@Path("subscribers")
public class SubscriberManagerService extends Resource {
	private SubscriberConfiguration config;
	private SubscriberManager subscriberManager;
	private Map<String, String> secKeys = new ConcurrentHashMap<String, String>();
	private final static Logger log = Logger.getLogger(SubscriberManagerService.class.getName());

	public SubscriberManagerService(SubscriberManager subscriberManager) throws ValidationException {
		this.subscriberManager = subscriberManager;
		this.config = SubscriberConfigManager.getInstance();
	}

	@PermitAll
	@GET
	@Path("")
	@Produces({ MediaType.APPLICATION_JSON })
	public List<Subscriber> getSubscribers() {
		return config.getSubscribers();
	}

	@PermitAll
	@POST
	@Path("")
	@Consumes({ MediaType.APPLICATION_JSON })
	public String addSubscriber(Subscriber subscriber) throws ValidationException {
		try {
			String subscriberId = this.subscriberManager.add(subscriber);
			return subscriberId;
		} finally {
			SubscriberConfigManager.serialize();
		}
	}

	@PermitAll
	@GET
	@Path("{id}")
	@Produces({ MediaType.APPLICATION_JSON })
	public Subscriber getSubscriber(@PathParam("id") String id) {
		int index = config.getSubscribers().indexOf(new Subscriber(id));
		return config.getSubscribers().get(index);
	}

	@PermitAll
	@PUT
	@Path("{id}")
	@Consumes({ MediaType.APPLICATION_JSON })
	public void updateSubscriber(@PathParam("id") String id, Subscriber subscriber) throws ValidationException {
		if (!id.equals(subscriber.getId()))
			throw new ValidationException("ID of subscriber does not match");

		try {
			this.subscriberManager.update(subscriber);
		} finally {
			SubscriberConfigManager.serialize();
		}
	}

	@PermitAll
	@DELETE
	@Path("{id}")
	public void deleteSubscriber(@PathParam("id") String id) throws ValidationException {
		try {
			this.subscriberManager.remove(id);
			deleteKeyStore(id);
			deleteTrustCert(id);
			File key = Paths.get(Environment.CERT_FOLDER, id).toFile();
			key.delete();
		} finally {
			SubscriberConfigManager.serialize();
		}
	}

	@POST
	@Path("{id}/certs/trust")
	@PermitAll
	@Consumes({ MediaType.APPLICATION_OCTET_STREAM })
	public void setTrustCert(@PathParam("id") String id, InputStream stream) throws ValidationException {
		if (stream == null) {
			throw new ValidationException("Failed to upload trust cert: No data given");
		}
		java.nio.file.Path folder = Paths.get(Environment.CERT_FOLDER, id);
		if (!folder.toFile().exists()) {
			folder.toFile().mkdirs();
		}
		java.nio.file.Path file = Paths.get(folder.toString(), Environment.TRUST_FILE_NAME);
		try {
			Files.copy(stream, file, StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			throw new ValidationException("Failed to upload trust cert: " + e.getMessage(), e);
		}

		Subscriber sub = subscriberManager.get(id);
		if (sub != null)
			subscriberManager.update(sub);
	}

	@GET
	@Path("{id}/certs/trust")
	@PermitAll
	public boolean hasTrusted(@PathParam("id") String id) {
		File key = Paths.get(Environment.CERT_FOLDER, id, Environment.TRUST_FILE_NAME).toFile();
		return key.exists();
	}

	@DELETE
	@Path("{id}/certs/trust")
	@PermitAll
	public void deleteTrustCert(@PathParam("id") String id) throws ValidationException {
		File key = Paths.get(Environment.CERT_FOLDER, id, Environment.TRUST_FILE_NAME).toFile();
		if (key.exists()) {
			key.delete();
		}
	}

	@POST
	@Path("{id}/certs/keystore")
	@PermitAll
	@Consumes({ MediaType.APPLICATION_OCTET_STREAM })
	public void setKeyStore(@PathParam("id") String id, @QueryParam("secKey") String secKey, InputStream stream) throws ValidationException {
		if (stream == null) {
			throw new ValidationException("Failed to upload key store: No data given");
		}
		try {
			KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
			KeyStore newKs = KeyStore.getInstance(KeyStore.getDefaultType());
			newKs.load(null, null);
			String passphrase = this.secKeys.remove(secKey);
			if (passphrase == null) {
				throw new ValidationException("No passphrase specified");
			}
			keyStore.load(stream, passphrase.toCharArray());

			Enumeration<String> aliases = keyStore.aliases();

			while (aliases.hasMoreElements()) {
				String alias = aliases.nextElement();
				Key privateKey = keyStore.getKey(alias, passphrase.toCharArray());
				java.security.cert.Certificate[] certificateChain = keyStore.getCertificateChain(alias);
				newKs.setKeyEntry(alias, privateKey, Environment.KEYSTORE_PASSWD.toCharArray(), certificateChain);
			}
			
			java.nio.file.Path folder = Paths.get(Environment.CERT_FOLDER, id);
			if (!folder.toFile().exists()) {
				folder.toFile().mkdirs();
			}
			File file = Paths.get(folder.toString(), Environment.KEYSTORE_FILE_NAME).toFile();
			FileOutputStream fop = new FileOutputStream(file);
				newKs.store(fop, Environment.KEYSTORE_PASSWD.toCharArray());
			fop.close();

			Subscriber sub = subscriberManager.get(id);
			if (sub != null) {
				subscriberManager.update(sub);
			}

		} catch (Exception e) {
			log.log(Level.SEVERE, "Failed to upload key store", e);
			throw new ValidationException("Failed to upload key store: " + e.getMessage(), e);
		}

	}

	@GET
	@Path("{id}/certs/keystore")
	@PermitAll
	public boolean hasKeyStore(@PathParam("id") String id) {
		File key = Paths.get(Environment.CERT_FOLDER, id, Environment.KEYSTORE_FILE_NAME).toFile();
		return key.exists();
	}

	@DELETE
	@Path("{id}/certs/keystore")
	@PermitAll
	public void deleteKeyStore(@PathParam("id") String id) throws ValidationException {
		File key = Paths.get(Environment.CERT_FOLDER, id, Environment.KEYSTORE_FILE_NAME).toFile();
		if (key.exists()) {
			key.delete();
		}
	}

	@POST
	@Path("certs/passphrase")
	@PermitAll
	@Consumes({ MediaType.TEXT_PLAIN })
	@Produces({ MediaType.TEXT_PLAIN })
	public String setPassphrase(String passphrase) throws ValidationException {
		String secKey = UUID.randomUUID().toString();
		this.secKeys.put(secKey, passphrase);
		return secKey;
	}

}
