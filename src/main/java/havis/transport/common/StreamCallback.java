package havis.transport.common;

import java.io.InputStream;

public interface StreamCallback {

	void arrived(String path, InputStream stream);
}