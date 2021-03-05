package havis.transport.common;

import havis.transport.Messenger;
import havis.transport.TransportException;
import havis.transport.Transporter;
import havis.transport.ValidationException;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.URI;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvResultSetWriter;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.CsvContext;

/**
 * JDBC transporter
 * 
 * @param <T>
 *            type of messages
 */
public class JdbcTransporter<T> extends PlainTransporter<T> {

	private final static Logger log = Logger.getLogger(JdbcTransporter.class.getName());

	private final static String INSERT = "INSERT INTO %s (%s) VALUES (%s)";
	private final static String SELECT = "SELECT %s FROM %s LIMIT ? OFFSET ?";
	private static final String DELETE = "DELETE FROM %s";
	private static final String DROP = "DROP TABLE %s";

	private String connectionString;
	private String identifierQuoteFormat;
	private String tableName;
	private boolean keepConnection;
	private String select, delete, drop;

	private Connection connection;
	private PreparedStatement preparedStatement;
	private String sqlStatement;

	private final static CellProcessor processor = new CellProcessor() {

		@SuppressWarnings("unchecked")
		@Override
		public String execute(Object value, CsvContext context) {
			if (value instanceof Clob) {
				Clob clob = (Clob) value;
				try {
					try (InputStream stream = clob.getAsciiStream()) {
						byte[] bytes = new byte[stream.available()];
						stream.read(bytes);
						return new String(bytes);
					}
				} catch (Exception e) {
					log.log(Level.FINE, "Failed to read column data", e);
				}
			}
			return null;
		}
	};

	private void initializeDrivers() {
		identifierQuoteFormat = "\"%s\""; // default
		try {
			String[] parts = this.connectionString.split(":", 3);
			if (parts != null && parts.length >= 2) {
				switch (parts[1]) {
				case "mysql":
					Class.forName("com.mysql.jdbc.Driver").newInstance();
					identifierQuoteFormat = "`%s`";
					break;
				case "sqlserver":
					Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance();
					identifierQuoteFormat = "[%s]";
					break;
				case "h2":
					// quoting makes identifiers case sensitive, so we don't allow quoting
					// see: http://h2database.com/html/grammar.html#name
					identifierQuoteFormat = "%s";
					break;
				}
			}
		} catch (Exception e) {
			// ignore
		}
	}

	@Override
	protected void init(URI uri, Map<String, String> properties, List<String> fields) throws ValidationException {
		if (uri == null)
			throw new ValidationException("URI must not be null");
		this.connectionString = uri.toString();
		String table = null, storage = null, init = null;
		boolean clear = false, drop = false;
		if (properties != null) {
			for (Entry<String, String> entry : properties.entrySet()) {
				String key = entry.getKey();
				if (key != null && key.startsWith(Transporter.PREFIX + "JDBC")) {
					switch (key) {
					case Messenger.JDBC_TABLE_NAME_PROPERTY:
						if (entry.getValue() == null || entry.getValue().trim().length() == 0)
							throw new ValidationException("Invalid '" + Messenger.JDBC_TABLE_NAME_PROPERTY + "' value '" + entry.getValue() + "'");
						table = entry.getValue();
						break;
					case Messenger.JDBC_KEEP_CONNECTION_PROPERTY:
						if (entry.getValue() == null || entry.getValue().trim().length() == 0)
							throw new ValidationException("Invalid '" + Messenger.JDBC_KEEP_CONNECTION_PROPERTY + "' value '" + entry.getValue() + "'");
						this.keepConnection = Boolean.TRUE.toString().equalsIgnoreCase(entry.getValue());
						break;
					case Messenger.JDBC_INIT_STATEMENT:
						if (entry.getValue() == null || entry.getValue().trim().length() == 0)
							throw new ValidationException("Invalid '" + Messenger.JDBC_INIT_STATEMENT + "' value '" + entry.getValue() + "'");
						init = entry.getValue();
						break;
					case Messenger.JDBC_STORAGE:
						if (entry.getValue() == null || entry.getValue().trim().length() == 0)
							throw new ValidationException("Invalid '" + Messenger.JDBC_STORAGE + "' value '" + entry.getValue() + "'");
						storage = entry.getValue();
						break;
					case Messenger.JDBC_CLEAR:
						if (entry.getValue() == null || entry.getValue().trim().length() == 0)
							throw new ValidationException("Invalid '" + Messenger.JDBC_CLEAR + "' value '" + entry.getValue() + "'");
						clear = Boolean.TRUE.toString().equalsIgnoreCase(entry.getValue());
						break;
					case Messenger.JDBC_DROP:
						if (entry.getValue() == null || entry.getValue().trim().length() == 0)
							throw new ValidationException("Invalid '" + Messenger.JDBC_DROP + "' value '" + entry.getValue() + "'");
						drop = Boolean.TRUE.toString().equalsIgnoreCase(entry.getValue());
						break;
					default:
						throw new ValidationException("Unknown property key '" + key + "'");
					}
				}
			}
		}
		initializeDrivers();
		if (table == null)
			throw new ValidationException("'" + Messenger.JDBC_TABLE_NAME_PROPERTY + "' must be set");
		this.tableName = String.format(identifierQuoteFormat, table);

		if (storage != null) {
			StringBuilder columns = new StringBuilder(fields.size() * 10);
			boolean first = true;
			for (String field : fields) {
				if (first)
					first = false;
				else {
					columns.append(", ");
				}
				columns.append(String.format(identifierQuoteFormat, field));
			}
			select = String.format(SELECT, columns.toString(), this.tableName);
			Storage.INSTANCE.put(storage, this);
			if (clear)
				delete = String.format(DELETE, this.tableName);
			if (drop)
				this.drop = String.format(DROP, this.tableName);
		}

		if (init != null) {
			try {
				connect();
				try (Statement stmt = connection.createStatement()) {
					stmt.execute(init);
				} finally {
					commit();
				}
			} catch (SQLException | TransportException e) {
				throw new ValidationException("Failed to execute initial statement: " + e.getMessage());
			}
		}
	}

	private void connect() throws TransportException {
		if (this.connection == null) {
			try {
				this.connection = DriverManager.getConnection(this.connectionString);
				this.connection.setAutoCommit(false);
			} catch (SQLException e) {
				disconnect();
				throw new TransportException("Failed to connect to '" + this.connectionString + "': " + e.getMessage());
			}
		}
	}

	private void prepare(String sql) throws TransportException {
		this.sqlStatement = sql;
		try {
			this.preparedStatement = this.connection.prepareStatement(this.sqlStatement);
		} catch (SQLException e) {
			disconnect();
			throw new TransportException("Failed to prepare statement '" + this.sqlStatement + "' on '" + this.connectionString + "': " + e.getMessage());
		}
	}

	private void setParameter(int parameterIndex, Object value) throws SQLException {
		if (value instanceof Date)
			this.preparedStatement.setTimestamp(parameterIndex, new Timestamp(((Date) value).getTime()));
		else
			this.preparedStatement.setObject(parameterIndex, value);
	}

	private void disconnect() {
		if (this.preparedStatement != null) {
			try {
				this.preparedStatement.close();
			} catch (SQLException e) {
				// ignore
			}
			this.preparedStatement = null;
		}
		if (this.connection != null) {
			try {
				this.connection.close();
			} catch (SQLException e) {
				// ignore
			}
			this.connection = null;
		}
	}

	@Override
	public void prepare(List<String> fields) throws TransportException {
		StringBuilder columns = new StringBuilder();
		StringBuilder values = new StringBuilder();
		boolean first = true;
		for (String field : fields) {
			if (first)
				first = false;
			else {
				columns.append(", ");
				values.append(", ");
			}
			columns.append(String.format(identifierQuoteFormat, field));
			values.append('?');
		}
		connect();
		prepare(String.format(INSERT, this.tableName, columns.toString(), values.toString()));
	}

	@Override
	public void write(List<Object> values) throws TransportException {
		if (this.connection == null)
			throw new TransportException("Connection is not open for execution");
		if (this.preparedStatement == null)
			throw new TransportException("Statement was not prepared for execution");
		try {
			for (int i = 0; i < values.size(); i++) {
				setParameter(i + 1, values.get(i));
			}
			this.preparedStatement.execute();
			this.preparedStatement.clearParameters();
		} catch (SQLException e) {
			disconnect();
			throw new TransportException("Failed to execute statement '" + this.sqlStatement + "' on '" + this.connectionString + "': " + e.getMessage());
		}
	}

	@Override
	public void commit() throws TransportException {
		if (this.connection == null)
			throw new TransportException("Connection is not open for execution");
		try {
			this.connection.commit();
		} catch (SQLException e) {
			throw new TransportException("Failed to commit on connection '" + this.connectionString + "': " + e.getMessage());
		} finally {
			if (!this.keepConnection) {
				disconnect();
			}
		}
	}

	@Override
	public void dispose() {
		if (drop != null) {
			try {
				connect();

				try (Statement statement = connection.createStatement()) {
					statement.execute(drop);
				} finally {
					commit();
				}
			} catch (TransportException | SQLException e) {
				log.log(Level.SEVERE, "Failed to drop table: " + e.getMessage());
			}
		}

		disconnect();
	}

	public void marshal(Writer writer, int limit, int offset) throws TransportException {
		if (select != null) {
			connect();

			try (PreparedStatement stmt = connection.prepareStatement(select)) {
				stmt.setInt(1, limit);
				stmt.setInt(2, offset);
				try (ResultSet rs = stmt.executeQuery()) {
					ResultSetMetaData data = rs.getMetaData();
					CellProcessor[] processors = new CellProcessor[data.getColumnCount()];
					for (int i = 0; i < data.getColumnCount(); i++)
						if (data.getColumnType(i + 1) == Types.CLOB)
							processors[i] = processor;
					try (CsvResultSetWriter csv = new CsvResultSetWriter(writer, CsvPreference.EXCEL_PREFERENCE)) {
						csv.write(rs, processors);
						csv.flush();
					}
				}
			} catch (IOException | SQLException e) {
				throw new TransportException("Failed to stream data: " + e.getMessage());
			}
		}
	}

	public int clear() throws TransportException {
		if (delete != null) {
			connect();

			try {
				try (Statement stmt = connection.createStatement()) {
					return stmt.executeUpdate(delete);
				} finally {
					commit();
				}
			} catch (SQLException e) {
				throw new TransportException("Failed to clear data: " + e.getMessage());
			}
		}
		return -1;
	}
}
