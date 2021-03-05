package havis.transport.common;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.UUID;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

public class SSLContextManager {

	private static SSLContext trustAllContext;
	private static HostnameVerifier trustAllVerifier;

	static {
		try {
			// TODO SSL/TLS Version
			trustAllContext = SSLContext.getInstance("SSL");
			trustAllContext.init(null, new TrustManager[] { new X509TrustManager() {
				@Override
				public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
				}

				@Override
				public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
				}

				@Override
				public X509Certificate[] getAcceptedIssuers() {
					return null;
				}
			} }, null);
			trustAllVerifier = new HostnameVerifier() {
				@Override
				public boolean verify(String hostname, SSLSession session) {
					return true;
				}
			};
		} catch (Exception e) {
			// ignore
		}
	}

	public synchronized static SSLSocketFactory createSSLSocketFactory(String subscriberId, String protocol) throws KeyStoreException, NoSuchAlgorithmException,
			CertificateException, FileNotFoundException, IOException, UnrecoverableKeyException, KeyManagementException {

		KeyManager[] kms = null;
		TrustManager[] tms = null;

		// Client Key Store - only if Client Store is set
		Path keyPath = Paths.get(Environment.CERT_FOLDER, subscriberId, Environment.KEYSTORE_FILE_NAME);
		if (keyPath.toFile().exists()) {
			KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
			//TODO MICA depended passwd
			try (InputStream is = new FileInputStream(keyPath.toString())) {
				keyStore.load(is, Environment.KEYSTORE_PASSWD.toCharArray());
			}
			KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
			kmf.init(keyStore, Environment.KEYSTORE_PASSWD.toCharArray());
			kms = kmf.getKeyManagers();
		}

		// Trusted Key Store - only if Trusted Cert is set
		Path trustedPath = Paths.get(Environment.CERT_FOLDER, subscriberId, Environment.TRUST_FILE_NAME);
		if (trustedPath.toFile().exists()) {
			TrustManagerFactory tm = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
			KeyStore trusted = KeyStore.getInstance(KeyStore.getDefaultType());
			trusted.load(null, null);
			insertTrusted(trusted, trustedPath.toString());
			tm.init(trusted);
			tms = tm.getTrustManagers();
		}

		if (kms == null && tms == null) {
			return null;
		} else {
			SSLContext sslcontext = SSLContext.getInstance(protocol);
			sslcontext.init(kms, tms, new SecureRandom());
			return sslcontext.getSocketFactory();
		}		
	}

	public synchronized static SSLSocketFactory createTrustedAllContext() {
		return trustAllContext != null ? trustAllContext.getSocketFactory() : null;
	}

	public synchronized static HostnameVerifier createTrustAllVerifier() {
		return trustAllVerifier;
	}

	private static void insertTrusted(KeyStore keyStore, String fileName) {
		try {
			CertificateFactory cf = CertificateFactory.getInstance("X.509");
			InputStream inputStream = new FileInputStream(fileName);
			Certificate cert;
			try {
				cert = cf.generateCertificate(inputStream);
			} finally {
				inputStream.close();
			}
			keyStore.setCertificateEntry(UUID.randomUUID().toString(), cert);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
