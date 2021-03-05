package havis.transport.common;

import havis.transport.DataWriter;
import havis.transport.TransportException;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ConsoleWriter implements DataWriter {

	private List<String> fields;

	private static void fixDates(Map<String, Object> map) {
		if (map != null) {
			for (Entry<String, Object> entry : map.entrySet()) {
				if (entry.getValue() instanceof Map) {
					@SuppressWarnings("unchecked")
					Map<String, Object> m = (Map<String, Object>) entry.getValue();
					if (m.size() == 1) {
						Entry<String, Object> e = m.entrySet().iterator().next();
						if (PlainDataConverter.DATE_IDENTIFIER.equals(e.getKey()) && e.getValue() instanceof Long) {
							entry.setValue(new Date(((Long) e.getValue()).longValue()));
							continue;
						}
					}
					fixDates(m);
				}
			}
		}
	}

	public static Object print(Object object) {
		if (object != null) {
			@SuppressWarnings("unchecked")
			Map<String, Object> map = PlainDataConverter.mapper.convertValue(object, Map.class);
			fixDates(map);
			System.out.println(map);
		}
		return object;
	}

	@Override
	public void prepare(List<String> fields) throws TransportException {
		this.fields = new ArrayList<>(fields);
	}

	@Override
	public void write(List<Object> values) throws TransportException {
		Iterator<String> f = fields.iterator();
		Iterator<Object> v = values.iterator();
		while (f.hasNext() && v.hasNext()) {
			System.out.println(f.next() + "=" + v.next());
		}
		System.out.println();
	}

	@Override
	public void commit() throws TransportException {
	}
}
