package havis.transport.common;

import havis.transport.Marshaller;
import havis.transport.Messenger;
import havis.transport.TransportException;
import havis.transport.ValidationException;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;

public class StreamFactory<T> {

	private Class<T> clazz;
	private String mimeType;
	private MediaType mediaType;
	private MessageBodyReader<T> reader;
	private MessageBodyWriter<T> writer;

	/**
	 * Initialize the message writer
	 * 
	 * @param clazz
	 *            the class of messages to send
	 * @param defaultMimeType
	 *            the default MIME type to use if no MIME type is specified in
	 *            the properties
	 * @param properties
	 *            the properties to use
	 * @throws ValidationException
	 *             if arguments are invalid
	 */
	public void init(Class<T> clazz, String defaultMimeType, Map<String, String> properties) throws ValidationException {
		if (clazz == null)
			throw new ValidationException("clazz must not be null");
		if (defaultMimeType == null)
			throw new ValidationException("defaultMimeType must not be null");
		this.clazz = clazz;
		if (properties != null) {
			for (Entry<String, String> entry : properties.entrySet()) {
				if (entry.getKey() != null) {
					switch (entry.getKey()) {
					case Messenger.MIMETYPE_PROPERTY:
						mimeType = entry.getValue();
						break;
					}
				}
			}
		}

		if (mimeType == null)
			mimeType = defaultMimeType;
	}

	private void initMediaType() throws TransportException {
		if (mediaType == null) {
			try {
				mediaType = MediaType.valueOf(mimeType);
			} catch (Throwable t) {
				throw new TransportException("Couldn't find media type '" + mimeType + "': " + t.toString(), t);
			}
		}
	}

	private MessageBodyReader<T> getReader() throws TransportException {
		initMediaType();
		if (reader == null)
			reader = Provider.getFactory().getMessageBodyReader(clazz, null, clazz.getAnnotations(), mediaType);
		if (reader == null)
			throw new TransportException("Couldn't find reader for media type '" + mediaType + "' and class '" + clazz.getName() + "'");
		return reader;
	}

	private MessageBodyWriter<T> getWriter() throws TransportException {
		initMediaType();
		if (writer == null)
			writer = Provider.getFactory().getMessageBodyWriter(clazz, null, clazz.getAnnotations(), mediaType);
		if (writer == null)
			throw new TransportException("Couldn't find writer for media type '" + mediaType + "' and class '" + clazz.getName() + "'");
		return writer;
	}

	/**
	 * Get a marshaller for the specified message
	 * 
	 * @return the marshaller
	 */
	public Marshaller<T> getMarshaller() {
		return new Marshaller<T>() {
			@Override
			public T unmarshal(InputStream source) throws TransportException {
				try {
					return Provider.getFactory().read(getReader(), clazz, clazz, clazz.getAnnotations(), mediaType, null, source);
				} catch (TransportException e) {
					throw e;
				} catch (Exception e) {
					throw new TransportException("Failed to read message from input stream", e);
				}
			}

			@Override
			public void marshal(T message, OutputStream target) throws TransportException {
				try {
					Provider.getFactory().write(getWriter(), message, clazz, null, clazz.getAnnotations(), mediaType, null, target);
				} catch (TransportException e) {
					throw e;
				} catch (Exception e) {
					throw new TransportException("Failed to write message to output stream", e);
				}
			}
		};
	}

}
