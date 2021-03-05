package havis.transport.common;

import havis.transport.DataConverter;
import havis.transport.DataWriter;
import havis.transport.Messenger;
import havis.transport.TransportException;
import havis.transport.Transporter;
import havis.transport.ValidationException;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * Data converter to create flat data from complex objects using an expression
 */
public class PlainDataConverter implements DataConverter {

	protected final static String DATE_IDENTIFIER = PlainDataConverter.class.getName() + "#" + Date.class.getName();

	protected final static ObjectMapper mapper = new ObjectMapper();
	static {
		SimpleModule dateModule = new SimpleModule();
		dateModule.addSerializer(Date.class, new JsonSerializer<Date>() {
			@Override
			public void serialize(Date value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
				jgen.writeNumberField(DATE_IDENTIFIER, value.getTime());
				jgen.writeEndObject();
			}

		});
		mapper.registerModule(dateModule);
	}

	protected static Object processDateValue(Object object) {
		if (object instanceof Map) {
			@SuppressWarnings("unchecked")
			Map<String, Object> map = (Map<String, Object>) object;
			if (map.size() == 1) {
				Entry<String, Object> entry = map.entrySet().iterator().next();
				if (DATE_IDENTIFIER.equals(entry.getKey()) && entry.getValue() instanceof Long) {
					return new Date(((Long) entry.getValue()).longValue());
				}
			}
		}
		return object;
	}

	private Identifier rootIdentifier;
	private List<String> fields;
	private boolean avoidDuplicates;

	@Override
	public void init(Map<String, String> properties) throws ValidationException {
		String expression = null;
		if (properties != null) {
			for (Entry<String, String> entry : properties.entrySet()) {
				String key = entry.getKey();
				if (key != null && key.startsWith(Transporter.PREFIX + "DataConverter")) {
					switch (key) {
					case Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY:
						expression = entry.getValue();
						break;
					case Messenger.DATA_CONVERTER_AVOID_DUPLICATES_PROPERTY:
						this.avoidDuplicates = Boolean.TRUE.toString().equalsIgnoreCase(entry.getValue());
						break;
					default:
						throw new ValidationException("Unknown property key '" + entry.getKey() + "'");
					}
				}
			}
		}
		if (expression == null)
			throw new ValidationException("'" + Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY + "' must be set to convert to flat data");
		parseExpression(expression);
	}

	private static class ProcessState {
		private Map<Field, AtomicInteger> fieldIndices = new HashMap<Field, AtomicInteger>();

		public Map<Field, AtomicInteger> getFieldIndices() {
			return fieldIndices;
		}
	}

	private static class FilteredArrayList extends ArrayList<Object> {
		private static final long serialVersionUID = 1L;
	}

	private static class Item {

		private static ThreadLocal<DateFormat> format = new ThreadLocal<DateFormat>() {
			@Override
			protected DateFormat initialValue() {
				return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
			}
		};

		private int index;
		private String matchFieldName;
		private Pattern matchPattern;

		public Item(int index) throws ValidationException {
			if (index < 0)
				throw new ValidationException("index must be greater zero");
			this.index = index;
		}

		public Item(String fieldName, String regex) throws ValidationException {
			this.index = -1;
			if (fieldName == null || fieldName.length() == 0)
				throw new ValidationException("fieldName must not be null or empty");
			if (regex == null || regex.length() == 0)
				throw new ValidationException("regex must not be null or empty");
			this.matchFieldName = fieldName;
			try {
				this.matchPattern = Pattern.compile(regex);
			} catch (PatternSyntaxException e) {
				throw new ValidationException("Failed to parse expression, invalid regular expression '" + regex + "': " + e.getMessage(), e);
			}
		}

		public List<?> filter(List<?> list, Field field) throws TransportException {
			FilteredArrayList filtered = new FilteredArrayList();
			if (this.index == -1) {
				for (Object item : list) {
					Object listItem = processDateValue(item);
					if (listItem instanceof Map) {
						// only sub items
						@SuppressWarnings("unchecked")
						Map<String, Object> subItem = (Map<String, Object>) listItem;
						if (!subItem.containsKey(this.matchFieldName))
							continue;

						Object subItemValue = processDateValue(subItem.get(this.matchFieldName));
						String matchValue = subItemValue != null ? handleDate(subItemValue).toString() : "";
						if (this.matchPattern.matcher(matchValue).matches()) {
							filtered.add(listItem);
						}
					}
				}
				if (filtered.size() == 0 && !field.isOptional())
					throw new TransportException("No result after filtering non-optional list field '" + field.getName() + "' with pattern '"
							+ this.matchFieldName + "=" + this.matchPattern.pattern() + "'");
			} else {
				if (this.index > list.size() - 1) {
					if (!field.isOptional())
						throw new TransportException("Non-optional list field '" + field.getName() + "' has no entry for index " + this.index);
				} else
					filtered.add(list.get(this.index));
			}

			return filtered;
		}

		private Object handleDate(Object value) {
			if (value instanceof Date)
				return format.get().format((Date) value);
			return value;
		}
	}

	private static class Field {

		private int id;
		private String name;
		private Item item;
		private String alias;
		private boolean optional;

		public Field(int id, String name, Item item, String alias, boolean optional) {
			this.id = id;
			this.name = name;
			this.item = item;
			this.alias = alias;
			this.optional = optional;
		}

		public String getName() {
			return name;
		}

		public Item getItem() {
			return item;
		}

		public String getAlias() {
			return alias;
		}

		public boolean isOptional() {
			return optional;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + id;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (!(obj instanceof Field))
				return false;
			Field other = (Field) obj;
			if (id != other.id)
				return false;
			return true;
		}
	}

	private static class Identifier {

		private Set<Field> fieldsWithAlias;
		private Map<Field, Identifier> subIdentifiers;

		public void addField(int fieldId, String fieldName, Item fieldItem, String alias, boolean optional) throws ValidationException {
			if (this.fieldsWithAlias == null)
				this.fieldsWithAlias = new LinkedHashSet<>();
			if (fieldName == null || fieldName.length() == 0)
				throw new ValidationException("fieldName must be specified");
			this.fieldsWithAlias.add(new Field(fieldId, fieldName, fieldItem, alias != null ? alias : fieldName, optional));
		}

		public Identifier addSubIdentifier(int fieldId, String fieldName, Item fieldItem, boolean optional, Identifier sub) throws ValidationException {
			if (this.subIdentifiers == null)
				this.subIdentifiers = new LinkedHashMap<>();
			if (fieldName == null || fieldName.length() == 0)
				throw new ValidationException("fieldName must be specified");
			if (sub == null)
				throw new ValidationException("sub must not be null");
			this.subIdentifiers.put(new Field(fieldId, fieldName, fieldItem, null, optional), sub);
			return sub;
		}

		public List<String> getFields() throws ValidationException {
			Set<String> fields = new LinkedHashSet<>();
			getFields(fields);
			return new ArrayList<String>(fields);
		}

		private void getFields(Set<String> fields) {
			if (this.fieldsWithAlias != null) {
				for (Field field : this.fieldsWithAlias) {
					if (!fields.contains(field.getAlias())) {
						// duplicates are OK, values will be overwritten when
						// data is flattened
						fields.add(field.getAlias());
					}
				}
			}
			if (this.subIdentifiers != null) {
				for (Entry<Field, Identifier> sub : this.subIdentifiers.entrySet()) {
					sub.getValue().getFields(fields);
				}
			}
		}

		public void set(Map<String, Object> treeSource, List<String> fields, DataWriter writer) throws TransportException {
			ProcessState state = new ProcessState();
			while (set(state, treeSource, fields, writer)) {
			}
			writer.commit();
		}

		private boolean set(ProcessState state, Map<String, Object> treeSource, List<String> fields, DataWriter writer) throws TransportException {
			Map<String, Object> data = new HashMap<>();
			boolean hasMore = setValues(state, treeSource, data);
			List<Object> values = new ArrayList<>(fields.size());
			for (String field : fields)
				values.add(data.get(field));
			writer.write(values);
			return hasMore;
		}

		private boolean setValues(ProcessState state, Map<String, Object> treeSource, Map<String, Object> values) throws TransportException {
			AtomicBoolean result = new AtomicBoolean();
			if (this.fieldsWithAlias != null) {
				for (Field field : this.fieldsWithAlias) {
					Object value = null;
					if (treeSource.containsKey(field.getName())) {
						Object object = processDateValue(treeSource.get(field.getName()));
						if (object instanceof Map)
							throw new TransportException("Field '" + field.getName() + "' is a complex type and cannot be used directly");
						value = processDateValue(processList(state, true, field, object, treeSource, result));
					} else if (!field.isOptional())
						throw new TransportException("Failed to find non-optional field '" + field.getName() + "'");

					values.put(field.getAlias(), value);
				}
			}
			if (this.subIdentifiers != null) {
				for (Entry<Field, Identifier> sub : this.subIdentifiers.entrySet()) {
					if (treeSource.containsKey(sub.getKey().getName())) {
						AtomicBoolean listHasMore = new AtomicBoolean();
						Object value = processList(state, false, sub.getKey(), treeSource.get(sub.getKey().getName()), treeSource, listHasMore);
						result.compareAndSet(false, listHasMore.get());

						if (value instanceof Map) {
							@SuppressWarnings("unchecked")
							Map<String, Object> subMap = (Map<String, Object>) value;

							boolean subHasMore = sub.getValue().setValues(state, subMap, values);
							if (!subHasMore) {
								incrementList(state, sub.getKey(), listHasMore.get());
							}
							result.compareAndSet(false, subHasMore);
						} else if (!sub.getKey().isOptional())
							throw new TransportException("Non-optional field '" + sub.getKey().getName()
									+ "' is not a complex type and cannot be sub processed");
					} else if (!sub.getKey().isOptional())
						throw new TransportException("Failed to find non-optional field '" + sub.getKey().getName() + "' for sub processing");
				}
			}

			return result.get();
		}

		private Object processList(ProcessState state, boolean increment, Field field, Object value, Map<String, Object> treeSource, AtomicBoolean hasMore)
				throws TransportException {
			Object result = value;
			if (value instanceof List) {
				List<?> list = (List<?>) value;
				if (field.getItem() != null) {
					list = field.getItem().filter(list, field);
				}
				if (!state.getFieldIndices().containsKey(field))
					state.getFieldIndices().put(field, new AtomicInteger());

				AtomicInteger index = state.getFieldIndices().get(field);
				if (index.get() <= list.size() - 1)
					result = list.get(index.get());
				else
					result = null;
				// true, if there's more entries
				hasMore.compareAndSet(false, (index.get() + 1) <= list.size() - 1);

				if (increment) {
					incrementList(state, field, hasMore.get());
				}
			}
			return result;
		}

		private void incrementList(ProcessState state, Field field, boolean hasMore) {
			if (state.getFieldIndices().containsKey(field)) {
				if (hasMore) {
					state.getFieldIndices().get(field).incrementAndGet();
				} else {
					state.getFieldIndices().get(field).set(0); // cycle
				}
			}
		}
	}

	private void parseExpression(String expression) throws ValidationException {
		this.rootIdentifier = parse(expression, new Identifier(), new AtomicInteger(0));
		this.fields = this.rootIdentifier.getFields();
	}

	private Identifier parse(String expression, Identifier current, AtomicInteger id) throws ValidationException {
		StringBuilder currentFieldName = new StringBuilder();
		StringBuilder currentSub = new StringBuilder();
		int level = 0;
		boolean lastWasClosingBracket = false;
		boolean inQuote = false;
		for (int i = 0; i < expression.length(); i++) {
			char c = expression.charAt(i);
			boolean isWhitespace = Character.isWhitespace(c);
			boolean isQuote = c == '"';
			if (isQuote)
				inQuote = !inQuote;

			switch (c) {
			case ',':
				if (!inQuote) {
					if (level > 0) {
						currentSub.append(c);
					} else if (currentFieldName.length() > 0) {
						String alias = parseAlias(currentFieldName);
						boolean optional = parseOptional(currentFieldName);
						Item item = parseItemExpression(currentFieldName);
						current.addField(id.getAndIncrement(), currentFieldName.toString().trim(), item, alias, optional);
						currentFieldName.setLength(0);
					} else if (!lastWasClosingBracket)
						throw new ValidationException("Failed to parse expression, unexpected comma near: " + createNear(expression, i));
					break;
				}
			case '(':
				if (!inQuote) {
					if (level > 0)
						currentSub.append(c);
					level++;
					break;
				}
			case ')':
				if (!inQuote) {
					if (level == 0)
						throw new ValidationException("Failed to parse expression, unexpected closing bracket near: " + createNear(expression, i));
					level--;
					if (level > 0)
						currentSub.append(c);
					else {
						boolean optional = parseOptional(currentSub);
						Item item = parseItemExpression(currentSub);
						String fromField = parseFromField(currentSub);
						String subExpression = currentSub.toString().trim();
						parse(subExpression, current.addSubIdentifier(id.getAndIncrement(), fromField, item, optional, new Identifier()), id);
						currentSub.setLength(0);
					}
					break;
				}
			default:
				if (level == 0) {
					if (currentFieldName.length() > 0 ||
					/* skip leading spaces */
					!isWhitespace)
						currentFieldName.append(c);
				} else
					currentSub.append(c);
				break;
			}

			if (!isWhitespace)
				lastWasClosingBracket = !inQuote && (c == ')');
		}
		if (currentSub.length() > 0)
			if (inQuote)
				throw new ValidationException("Failed to parse expression, missing end quote in sub expression: " + createNear(currentSub.toString(), 0));
			else
				throw new ValidationException("Failed to parse expression, missing closing bracket for sub expression: " + createNear(currentSub.toString(), 0));
		if (currentFieldName.length() > 0) {
			String alias = parseAlias(currentFieldName);
			boolean optional = parseOptional(currentFieldName);
			Item item = parseItemExpression(currentFieldName);
			current.addField(id.getAndIncrement(), currentFieldName.toString().trim(), item, alias, optional);
			currentFieldName.setLength(0);
		}

		return current;
	}

	private String createNear(String expression, int index) {
		if (expression == null || expression.length() == 0)
			return "<unknown>";
		int start = Math.max(index - 15, 0);
		int end = Math.min(index + 15, expression.length());
		return (index > 0 ? "..." : "") + expression.substring(start, end) + "...";
	}

	private Item parseItemExpression(StringBuilder expression) throws ValidationException {
		int open = -1;
		int close = -1;
		for (int i = expression.length() - 1; i >= 0; i--) {
			char c = expression.charAt(i);
			if (close == -1 && Character.isWhitespace(c))
				continue;
			else if (c == ']')
				close = i;
			else if (close > -1) {
				if (c == '[') {
					open = i;
					break;
				} else
					continue;
			} else
				break;
		}
		if (open > -1 && close > -1) {
			String item = expression.substring(open + 1, close).trim();
			if (item.length() == 0)
				throw new ValidationException("Failed to parse expression, nothing specified for filter: "
						+ createNear(expression.toString(), expression.length()));

			boolean extended = false;
			for (int i = 0; i < item.length(); i++) {
				if (item.charAt(i) < '0' || item.charAt(i) > '9') {
					extended = true;
					break;
				}
			}

			if (extended) {
				int index = item.indexOf('=');
				if (index == -1)
					throw new ValidationException("Failed to parse expression, no equals sign specified for filter: "
							+ createNear(expression.toString(), expression.length()));
				String matchFieldName = item.substring(0, index).trim();
				String matchValue = unquote(item.substring(index + 1, item.length()).trim());
				if (matchFieldName.length() == 0)
					throw new ValidationException("Failed to parse expression, no field name specified for filter: "
							+ createNear(expression.toString(), expression.length()));
				if (matchValue.length() == 0)
					throw new ValidationException("Failed to parse expression, no regular expression specified for filter: "
							+ createNear(expression.toString(), expression.length()));
				expression.delete(open, close + 1);
				return new Item(matchFieldName, matchValue);
			} else {
				expression.delete(open, close + 1);
				return new Item(Integer.parseInt(item));
			}
		} else if (open > -1 || close > -1)
			throw new ValidationException("Failed to parse expression, missing square bracket for filter: "
					+ createNear(expression.toString(), expression.length()));
		return null;
	}

	private String unquote(String string) throws ValidationException {
		if (string.length() > 0) {
			int start = 0, end = string.length();
			int found = 0;
			if (string.charAt(0) == '"') {
				start = 1;
				found++;
			}
			if (string.charAt(string.length() - 1) == '"') {
				end = string.length() - 1;
				found++;
			}
			if (found == 1)
				throw new ValidationException("Failed to parse expression, missing quote for expression: " + string);
			String result = string.substring(start, end);
			if (found == 2) // if inside quotes, unescape double quotes
				result = result.replace("\"\"", "\"");
			return result;
		}
		return string;
	}

	private String parseAlias(StringBuilder expression) throws ValidationException {
		int index = expression.lastIndexOf(" as ");
		if (index == -1)
			return null;
		String alias = expression.substring(index + 4, expression.length()).trim();
		expression.delete(index, expression.length());
		return unquote(alias);
	}

	private boolean parseOptional(StringBuilder expression) {
		int index = -1;
		for (int i = expression.length() - 1; i >= 0; i--) {
			char c = expression.charAt(i);
			if (Character.isWhitespace(c))
				continue;
			else if (c == '?') {
				index = i;
				break;
			} else
				break;
		}
		if (index > 0) {
			expression.delete(index, expression.length());
		}
		return index > 0;
	}

	private String parseFromField(StringBuilder subExpression) throws ValidationException {
		int lastBracketIndex = subExpression.lastIndexOf(")");
		int index = subExpression.lastIndexOf(" in ");
		if (lastBracketIndex == subExpression.length() - 1)
			throw new ValidationException("Failed to parse expression, missing 'in' expression, maybe unexpected closing bracket near: "
					+ createNear(subExpression.toString(), subExpression.length()));
		if (index == -1 || index < lastBracketIndex)
			throw new ValidationException("Failed to parse expression, missing 'in' keyword near: "
					+ createNear(subExpression.toString(), subExpression.length()));
		String fromField = subExpression.substring(index + 4, subExpression.length()).trim();
		if (fromField.indexOf("[") > -1 || fromField.indexOf("]") > -1)
			throw new ValidationException("Failed to parse expression, irregular square brackets near: "
					+ createNear(subExpression.toString(), subExpression.length()));
		subExpression.delete(index, subExpression.length());
		return fromField;
	}

	@Override
	public void convert(Object message, DataWriter writer) throws TransportException {
		if (writer == null)
			throw new TransportException("writer must not be null");
		writer = handleDuplicates(writer);
		writer.prepare(this.fields);
		Map<String, Object> treeSource = null;
		try {
			@SuppressWarnings("unchecked")
			Map<String, Object> result = mapper.convertValue(message, Map.class);
			treeSource = result;
		} catch (Exception e) {
			throw new TransportException("Failed to convert message to Map using ObjectMapper", e);
		}
		if (treeSource != null) {
			flatten(treeSource, this.fields, writer);
		}
	}

	private DataWriter handleDuplicates(final DataWriter writer) {
		if (this.avoidDuplicates) {
			return new DataWriter() {

				Set<List<Object>> data = new LinkedHashSet<>();

				@Override
				public void prepare(List<String> fields) throws TransportException {
					writer.prepare(fields);
				}

				@Override
				public void write(List<Object> values) throws TransportException {
					if (!data.contains(values))
						data.add(values);
				}

				@Override
				public void commit() throws TransportException {
					for (List<Object> values : data)
						writer.write(values);

					writer.commit();
				}
			};
		}
		return writer;
	}

	private void flatten(Map<String, Object> treeSource, List<String> fields, DataWriter writer) throws TransportException {
		this.rootIdentifier.set(treeSource, fields, writer);
	}

	public List<String> getFields() {
		return fields;
	}
}