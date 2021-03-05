package havis.transport.common;

import havis.middleware.ale.base.report.ReportConstants;
import havis.middleware.ale.service.ECReaderStat;
import havis.middleware.ale.service.ECSightingStat;
import havis.middleware.ale.service.EPC;
import havis.middleware.ale.service.ec.ECReport;
import havis.middleware.ale.service.ec.ECReportGroup;
import havis.middleware.ale.service.ec.ECReportGroupList;
import havis.middleware.ale.service.ec.ECReportGroupListMember;
import havis.middleware.ale.service.ec.ECReportGroupListMemberExtension;
import havis.middleware.ale.service.ec.ECReportGroupListMemberExtension.FieldList;
import havis.middleware.ale.service.ec.ECReportMemberField;
import havis.middleware.ale.service.ec.ECReports;
import havis.middleware.ale.service.ec.ECReports.Reports;
import havis.middleware.ale.service.ec.ECSightingSignalStat;
import havis.middleware.ale.service.ec.ECTagCountStat;
import havis.middleware.ale.service.ec.ECTagStat;
import havis.middleware.ale.service.ec.ECTagTimestampStat;
import havis.transport.DataConverter;
import havis.transport.DataWriter;
import havis.transport.Messenger;
import havis.transport.TransportException;
import havis.transport.ValidationException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import mockit.Capturing;
import mockit.Verifications;

public class PlainDataConverterTest {

	@Test
	public void convertDummy(@Capturing final DataWriter dw) throws Exception {
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "fname, lname as \"Last Name\", age, (lname as ContactName in contacts)");
		converter.init(properties);
		converter.convert(new Dummy("Tester", "Test", 1, createDummyArray(), null), dw);

		// Verifies that the writer methods are called a specific time with
		// expected values
		new Verifications() {
			{
				ArrayList<String> expectedFields = new ArrayList<String>(Arrays.asList("fname", "Last Name", "age", "ContactName"));
				List<String> fields;
				dw.prepare(fields = withCapture());
				times = 1;
				Assert.assertTrue(fields.containsAll(expectedFields));
				Assert.assertTrue(expectedFields.size() == fields.size());

				ArrayList<Object> expectedValues = new ArrayList<>();
				expectedValues.add("Test");
				expectedValues.add("Tester");
				expectedValues.add(Integer.valueOf(1));
				expectedValues.add("Brinkmann");
				List<Object> values;
				dw.write(values = withCapture());
				times = 3;
				Assert.assertTrue(values.containsAll(expectedValues));
				Assert.assertTrue(expectedValues.size() == values.size());

				dw.commit();
				times = 1;
			}
		};
	}

	@Test
	public void convertECReportWithSightingsAndTime() throws Exception {
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_AVOID_DUPLICATES_PROPERTY, Boolean.TRUE.toString());
		properties
				.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY,
						"((reportName, (groupName, (((value as epc in epc), (((value as tid in field[name=TID]?) in fieldList?), ((firstSightingTime as time in stat[profile=TagTimestamps]?), (((((antenna as antenna, strength as strength in sighting[0]) in sightings?) in statBlock[0]) in statBlocks?) in stat[profile=ReaderSightingSignals]?) in stats) in extension) in member) in groupList) in group) in report) in reports)");
		final AtomicInteger count = new AtomicInteger(0);

		DataConverter converter = new PlainDataConverter();
		converter.init(properties);
		converter.convert(createECReports(), new DataWriter() {

			@Override
			public void write(List<Object> values) throws TransportException {
				Assert.assertEquals(7, values.size());
				if (count.get() == 0) {
					Assert.assertEquals("reportA", values.get(0));
					Assert.assertEquals("Default", values.get(1));
					Assert.assertEquals("epc1", values.get(2));
					Assert.assertEquals("tidvalue1", values.get(3));
					Assert.assertEquals(new Date(0), values.get(4));
					Assert.assertEquals(1, values.get(5));
					Assert.assertEquals(10, values.get(6));
				} else {
					Assert.assertEquals("reportA", values.get(0));
					Assert.assertEquals("Default", values.get(1));
					Assert.assertEquals("epc2", values.get(2));
					Assert.assertEquals("tidvalue2", values.get(3));
					Assert.assertEquals(new Date(50000), values.get(4));
					Assert.assertEquals(1, values.get(5));
					Assert.assertEquals(10, values.get(6));
				}
				count.incrementAndGet();
			}

			@Override
			public void prepare(List<String> fields) throws TransportException {
				List<String> fieldList = Arrays.asList("reportName", "groupName", "epc", "tid", "time", "antenna", "strength");
				Assert.assertEquals(fieldList, fields);
			}

			@Override
			public void commit() throws TransportException {
			}
		});
	}

	@Test
	public void convertECReport() throws Exception {
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_AVOID_DUPLICATES_PROPERTY, Boolean.TRUE.toString());
		// properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY,
		// "specName, creationDate, ((reportName, (groupName, (((value as epcValue in epc) in member) in groupList) in group) in report) in reports)");
		// properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY,
		// "specName, creationDate, (((groupName, (((value as epcValue in epc), (((profile, count? as tagCount, firstSightingTime? as sightingFirst, lastSightingTime? as sightingLast, ((readerName as sightingReader, ((host as sightingHost, antenna as sightingAntenna, strength as sightingStrength in sighting) in sightings?) in statBlock) in statBlocks?) in stat) in stats) in extension) in member) in groupList) in group) in report) in reports)");
		properties
				.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY,
						"specName, creationDate, (((groupName, (((value as epcValue in epc), (((profile as profile1, firstSightingTime as sightingFirst, lastSightingTime as sightingLast in stat[profile=TagTimestamps]?), (profile as profile2, count as tagCount in stat[profile=TagCount]?), (profile as profile3, ((readerName as reader1 in statBlock[0]), (readerName as reader2 in statBlock[1]) in statBlocks) in stat[profile=ReaderNames]?) in stats) in extension) in member) in groupList) in group) in report) in reports)");
		final AtomicInteger count = new AtomicInteger(0);

		DataConverter converter = new PlainDataConverter();
		converter.init(properties);
		converter.convert(createECReports(), new DataWriter() {

			@Override
			public void write(List<Object> values) throws TransportException {
				Assert.assertEquals(12, values.size());
				if (count.get() == 0) {
					Assert.assertTrue(values.get(0).equals("specA"));
					Assert.assertTrue(values.get(1).equals(new Date(123456789)));
					Assert.assertTrue(values.get(2).equals("Default"));
					Assert.assertTrue(values.get(3).equals("epc1"));
					Assert.assertTrue(values.get(4).equals("TagTimestamps"));
					Assert.assertTrue(values.get(5).equals(new Date(0)));
					Assert.assertTrue(values.get(6).equals(new Date(1000)));
					Assert.assertTrue(values.get(7).equals("TagCount"));
					Assert.assertTrue(values.get(8).equals(Integer.valueOf(3)));
					Assert.assertTrue(values.get(9).equals("ReaderNames"));
					Assert.assertTrue(values.get(10).equals("reader1"));
					Assert.assertTrue(values.get(11).equals("reader2"));
				} else {
					Assert.assertTrue(values.get(0).equals("specA"));
					Assert.assertTrue(values.get(1).equals(new Date(123456789)));
					Assert.assertTrue(values.get(2).equals("Default"));
					Assert.assertTrue(values.get(3).equals("epc2"));
					Assert.assertTrue(values.get(4).equals("TagTimestamps"));
					Assert.assertTrue(values.get(5).equals(new Date(50000)));
					Assert.assertTrue(values.get(6).equals(new Date(200000)));
					Assert.assertTrue(values.get(7).equals("TagCount"));
					Assert.assertTrue(values.get(8).equals(Integer.valueOf(4)));
					Assert.assertTrue(values.get(9).equals("ReaderNames"));
					Assert.assertTrue(values.get(10).equals("readerX1"));
					Assert.assertTrue(values.get(11).equals("readerX2"));
				}
				count.incrementAndGet();
			}

			@Override
			public void prepare(List<String> fields) throws TransportException {
				List<String> fieldList = Arrays.asList("specName", "creationDate", "groupName", "epcValue", "profile1", "sightingFirst", "sightingLast",
						"profile2", "tagCount", "profile3", "reader1", "reader2");
				Assert.assertEquals(fieldList, fields);
			}

			@Override
			public void commit() throws TransportException {
			}
		});
	}

	@Test
	public void convertECReportDuplicates() throws Exception {
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "specName as a, creationDate as a");

		DataConverter converter = new PlainDataConverter();
		converter.init(properties);
		converter.convert(createECReports(), new DataWriter() {

			@Override
			public void write(List<Object> values) throws TransportException {
				Assert.assertEquals(1, values.size());
				Assert.assertTrue(values.get(0).equals(new Date(123456789)));
			}

			@Override
			public void prepare(List<String> fields) throws TransportException {
				List<String> fieldList = Arrays.asList("a");
				Assert.assertEquals(fieldList, fields);
			}

			@Override
			public void commit() throws TransportException {
			}
		});
	}

	@Test
	public void convertECReportEmpty() throws Exception {
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_AVOID_DUPLICATES_PROPERTY, Boolean.TRUE.toString());
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY,
				"specName, creationDate, ((reportName, (groupName, (((value as epcValue in epc) in member) in groupList) in group) in report?) in reports)");

		DataConverter converter = new PlainDataConverter();
		converter.init(properties);
		ECReports report = createECReports();
		report.getReports().getReport().clear();
		converter.convert(report, new DataWriter() {

			@Override
			public void write(List<Object> values) throws TransportException {
				Assert.assertEquals(5, values.size());
				Assert.assertTrue(values.get(0).equals("specA"));
				Assert.assertTrue(values.get(1).equals(new Date(123456789)));
				Assert.assertNull(values.get(2));
				Assert.assertNull(values.get(3));
				Assert.assertNull(values.get(4));
			}

			@Override
			public void prepare(List<String> fields) throws TransportException {
				List<String> fieldList = Arrays.asList("specName", "creationDate", "reportName", "groupName", "epcValue");
				Assert.assertEquals(fieldList, fields);
			}

			@Override
			public void commit() throws TransportException {
			}
		});
	}

	@Test
	public void initWithoutExpressionProperty() throws Exception {
		String expectedExceptionMessage = "'" + Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY + "' must be set to convert to flat data";
		Map<String, String> properties = new HashMap<>();
		DataConverter converter = new PlainDataConverter();

		try {
			converter.init(properties);
		} catch (havis.transport.ValidationException e) {
			Assert.assertEquals(expectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void initWithUnknownPropertyKey() throws Exception {
		String expectedExceptionMessage = "Unknown property key '" + Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY + "x" + "'";
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY + "x", "test");
		try {
			converter.init(properties);
		} catch (havis.transport.ValidationException e) {
			Assert.assertEquals(expectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void parseUnexpectedComma() {
		String expectedExceptionMessage = "Failed to parse expression, unexpected comma near: ...test,,abc,def...";
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "test,,abc,def");
		try {
			converter.init(properties);
		} catch (havis.transport.ValidationException e) {
			Assert.assertEquals(expectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void parseMissingClosingBracket() {
		String expectedExceptionMessage = "Failed to parse expression, missing closing bracket for sub expression: (abc in testing...";
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "test, ((abc in testing, in test2)");
		try {
			converter.init(properties);
		} catch (havis.transport.ValidationException e) {
			Assert.assertEquals(expectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void parseUnexpectedClosingBracket() {
		String expectedExceptionMessage = "Failed to parse expression, unexpected closing bracket near: ...sting) in test2)...";
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "test, (abc in testing) in test2)");
		try {
			converter.init(properties);
		} catch (havis.transport.ValidationException e) {
			Assert.assertEquals(expectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void parseMissingEndQuoteInSub() throws Exception {
		String ExpectedExceptionMessage = "Failed to parse expression, missing end quote in sub expression: fname in " + '"' + "conta...";
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "(fname in " + '"' + "contacts)");
		try {
			converter.init(properties);
		} catch (ValidationException e) {
			Assert.assertEquals(ExpectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void parseItemExpressionNoFilter() {
		String expectedExceptionMessage = "Failed to parse expression, nothing specified for filter: ...bc in testing[]...";
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "test, ((abc in testing[]) in test2)");
		try {
			converter.init(properties);
		} catch (havis.transport.ValidationException e) {
			Assert.assertEquals(expectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void parseItemExpressionNoEqualsSign() {
		String expectedExceptionMessage = "Failed to parse expression, no equals sign specified for filter: ...esting[value 5]...";
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "test, ((abc in testing[value 5]) in test2)");
		try {
			converter.init(properties);
		} catch (havis.transport.ValidationException e) {
			Assert.assertEquals(expectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void parseItemExpressionNoFieldName() {
		String expectedExceptionMessage = "Failed to parse expression, no field name specified for filter: ... in testing[=5]...";
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "test, ((abc in testing[=5]) in test2)");
		try {
			converter.init(properties);
		} catch (havis.transport.ValidationException e) {
			Assert.assertEquals(expectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void parseItemExpressionNoRegex() {
		String expectedExceptionMessage = "Failed to parse expression, no regular expression specified for filter: ...sting[value=" + '"' + '"' + "]...";
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		StringBuilder sb = new StringBuilder();
		sb.append("test, ((abc in testing[value=").append('"').append('"').append("]) in test2)");
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, sb.toString());
		try {
			converter.init(properties);
		} catch (havis.transport.ValidationException e) {
			Assert.assertEquals(expectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void parseItemExpressionMissingSquareBrackets() {
		String expectedExceptionMessage = "Failed to parse expression, missing square bracket for filter: ...testing5=value]...";
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "test, ((abc in testing5=value]) in test2)");
		try {
			converter.init(properties);
		} catch (havis.transport.ValidationException e) {
			Assert.assertEquals(expectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void unquoteMissingQuote() {
		String expectedExceptionMessage = "Failed to parse expression, missing quote for expression: " + '"' + "value";
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "test, ((abc in testing[" + '"' + "5=" + '"' + "value]) in test2)");
		try {
			converter.init(properties);
		} catch (havis.transport.ValidationException e) {
			Assert.assertEquals(expectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void unquoteMissingQuoteInAlias() {
		String expectedExceptionMessage = "Failed to parse expression, missing quote for expression: " + '"' + "Test";
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "test as " + '"' + "Test");
		try {
			converter.init(properties);
		} catch (havis.transport.ValidationException e) {
			Assert.assertEquals(expectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void unquoteMissingQuoteInAlias2() {
		String expectedExceptionMessage = "Failed to parse expression, missing quote for expression: Test" + '"';
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "test as Test" + '"');
		try {
			converter.init(properties);
		} catch (havis.transport.ValidationException e) {
			Assert.assertEquals(expectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void parseFromFieldMissingINBecauseOfClosingBracket() {
		String expectedExceptionMessage = "Failed to parse expression, missing 'in' expression, maybe unexpected closing bracket near: ...(abc as ABC in)...";
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "test, ((abc as ABC in)) test2");
		try {
			converter.init(properties);
		} catch (havis.transport.ValidationException e) {
			Assert.assertEquals(expectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void parseFromFieldMissingIN() {
		String expectedExceptionMessage = "Failed to parse expression, missing 'in' keyword near: ...abc as ABC...";
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "test, (abc as ABC) test2");
		try {
			converter.init(properties);
		} catch (havis.transport.ValidationException e) {
			Assert.assertEquals(expectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void parseFromFieldIrregularSquareBrackets() {
		String expectedExceptionMessage = "Failed to parse expression, irregular square brackets near: ...s ABC in te]st2...";
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "test, (abc as ABC in te]st2)");
		try {
			converter.init(properties);
		} catch (havis.transport.ValidationException e) {
			Assert.assertEquals(expectedExceptionMessage, e.getMessage());
		}
	}

	@Test(expected = NullPointerException.class)
	public void convertWithoutInit() throws Exception {
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "test, (abc as ABC in test2)");
		converter.convert(new Dummy("Tester", "Test", 1, null, null), new ConsoleWriter());
	}

	@Test
	public void convertWithoutWriter() throws Exception {
		String expectedExceptionMessage = "writer must not be null";
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "lname, fname, age");
		converter.init(properties);
		try {
			converter.convert(new Dummy("Tester", "Test", 1, null, null), null);
		} catch (TransportException e) {
			Assert.assertEquals(expectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void convertFailedToConvertMessageToMap() throws Exception {
		String ExpectedExceptionMessage = "Failed to convert message to Map using ObjectMapper";
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "test");
		converter.init(properties);
		try {
			converter.convert(Integer.valueOf(3), new ConsoleWriter());
		} catch (Exception e) {
			Assert.assertEquals(ExpectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void setValuesComplexTypeField() throws Exception {
		String ExpectedExceptionMessage = "Field 'complexType' is a complex type and cannot be used directly";
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "complexType");
		converter.init(properties);
		try {
			converter.convert(new Dummy("Tester", "Test", 21, null, null), new ConsoleWriter());
		} catch (Exception e) {
			Assert.assertEquals(ExpectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void setValuesNoNonOptionalField() throws Exception {
		String ExpectedExceptionMessage = "Failed to find non-optional field 'missingField'";
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "missingField");
		converter.init(properties);
		try {
			converter.convert(new Dummy("Tester", "Test", 21, null, null), new ConsoleWriter());
		} catch (Exception e) {
			Assert.assertEquals(ExpectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void setValuesSubFieldIsNotComplex() throws Exception {
		String ExpectedExceptionMessage = "Non-optional field 'lname' is not a complex type and cannot be sub processed";
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "(missing in lname)");
		converter.init(properties);
		try {
			converter.convert(new Dummy("Tester", "Test", 21, null, null), new ConsoleWriter());
		} catch (Exception e) {
			Assert.assertEquals(ExpectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void addSubIdentifierNoFieldname() throws Exception {
		String ExpectedExceptionMessage = "fieldName must be specified";
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "(abc in )");
		try {
			converter.init(properties);
		} catch (ValidationException e) {
			Assert.assertEquals(ExpectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void filterNoResultForAEqualFilter() throws Exception {
		String ExpectedExceptionMessage = "No result after filtering non-optional list field 'contacts' with pattern '100=age'";
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "(fname in contacts[100 = age])");
		converter.init(properties);
		try {
			converter.convert(new Dummy("Tester", "Test", 11, createDummyArray(), null), new ConsoleWriter());
		} catch (TransportException e) {
			Assert.assertEquals(ExpectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void filterNoResultInIndexRange() throws Exception {
		String ExpectedExceptionMessage = "Non-optional list field 'contacts' has no entry for index 10";
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "(fname in contacts[10])");
		converter.init(properties);
		try {
			converter.convert(new Dummy("Tester", "Test", 11, createDummyArray(), null), new ConsoleWriter());
		} catch (TransportException e) {
			Assert.assertEquals(ExpectedExceptionMessage, e.getMessage());
		}
	}

	@Test
	public void itemWithInvalidRegex() throws Exception {
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		String regex = "(fname in contacts[x=\"(xyz\"])";
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, regex);
		try {
			converter.init(properties);
		} catch (Exception e) {
			Assert.assertEquals("Failed to parse expression, invalid regular expression '(xyz': Unclosed group near index 4\n(xyz", e.getMessage());
		}
	}

	@Test
	public void handleDate() throws Exception {
		Dummy[] contacts = new Dummy[1];
		contacts[0] = new Dummy("NULL", "Test", 111, null, null);
		contacts[0].setLName(null);

		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "(born as Date in contacts[born=\"(.*)\"])");
		converter.init(properties);
		converter.convert(new Dummy("Tester", "Test", 100, contacts, null), new DataWriter() {

			@Override
			public void write(List<Object> values) throws TransportException {
				Assert.assertEquals(new Date(0), values.get(0));
			}

			@Override
			public void prepare(List<String> fields) throws TransportException {
				Assert.assertTrue(fields.contains("Date"));
			}

			@Override
			public void commit() throws TransportException {
			}
		});
	}

	@Test
	public void filterSubItemValueIsNull() throws Exception {
		Dummy[] contacts = new Dummy[1];
		contacts[0] = new Dummy("NULL", "Test", 111, null, null);
		contacts[0].setLName(null);

		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "(lname as LastName in contacts[lname=\"(.*)\"])");
		converter.init(properties);
		converter.convert(new Dummy("Tester", "Test", 100, contacts, null), new DataWriter() {

			@Override
			public void write(List<Object> values) throws TransportException {
				Assert.assertTrue(values.size() == 1);
				Assert.assertNull(values.get(0));
			}

			@Override
			public void prepare(List<String> fields) throws TransportException {
				Assert.assertTrue(fields.contains("LastName"));
			}

			@Override
			public void commit() throws TransportException {
			}
		});
	}

	@Test
	public void processListListsWithDifferentIndexesIncrement() throws Exception {
		final AtomicInteger countWrite = new AtomicInteger(0);

		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "lists");
		converter.init(properties);
		converter.convert(new Dummy("Tester", "Test", 100, createDummyArray(), null), new DataWriter() {

			@Override
			public void write(List<Object> values) throws TransportException {
				countWrite.set(countWrite.incrementAndGet());
			}

			@Override
			public void prepare(List<String> fields) throws TransportException {
				Assert.assertTrue(fields.contains("lists"));
			}

			@Override
			public void commit() throws TransportException {
			}
		});
		Assert.assertEquals(2, countWrite.get());
	}

	@Test
	public void processListTest() throws Exception {
		final AtomicInteger count = new AtomicInteger(0);
		DataConverter converter = new PlainDataConverter();
		Map<String, String> properties = new HashMap<>();
		properties.put(Messenger.DATA_CONVERTER_EXPRESSION_PROPERTY, "listOne, listTwo");
		converter.init(properties);
		converter.convert(new ListTest(2, 0), new DataWriter() {

			@Override
			public void write(List<Object> values) throws TransportException {
				switch (count.get()) {
				case 0:
					Assert.assertEquals(Integer.valueOf(0), values.get(0));
					Assert.assertNull(values.get(1));
					break;

				case 1:
					Assert.assertEquals(Integer.valueOf(1), values.get(0));
					Assert.assertNull(values.get(1));
					break;

				default:
					Assert.fail();
					break;
				}
				count.incrementAndGet();
			}

			@Override
			public void prepare(List<String> fields) throws TransportException {
				ArrayList<String> expectedFields = new ArrayList<String>(Arrays.asList("listOne", "listTwo"));
				Assert.assertTrue(fields.containsAll(expectedFields));
			}

			@Override
			public void commit() throws TransportException {
			}
		});
	}

	public static ECReports createECReports() {
		ECReports report = new ECReports();
		report.setSpecName("specA");
		report.setCreationDate(new Date(123456789));
		report.setReports(new Reports());
		ECReport r = new ECReport();
		r.setReportName("reportA");
		ECReportGroup g = new ECReportGroup();
		g.setGroupName("Default");
		ECReportGroupList l = new ECReportGroupList();
		ECReportGroupListMember m1 = new ECReportGroupListMember();
		m1.setEpc(new EPC("epc1"));
		ECReportGroupListMemberExtension e1 = new ECReportGroupListMemberExtension();

		e1.setFieldList(new FieldList());
		ECReportMemberField tid1 = new ECReportMemberField();
		tid1.setName("TID");
		tid1.setValue("tidvalue1");
		e1.getFieldList().getField().add(tid1);

		ArrayList<ECTagStat> stats1 = new ArrayList<ECTagStat>();
		ECTagTimestampStat time1 = new ECTagTimestampStat();
		time1.setProfile(ReportConstants.TagTimestampsProfileName);
		time1.setFirstSightingTime(new Date(0));
		time1.setLastSightingTime(new Date(1000));
		stats1.add(time1);

		ECTagCountStat count1 = new ECTagCountStat();
		count1.setProfile(ReportConstants.TagCountProfileName);
		count1.setCount(3);
		stats1.add(count1);

		ECTagStat names1 = new ECTagStat();
		names1.setProfile(ReportConstants.ReaderNamesProfileName);
		ArrayList<ECReaderStat> readerStats1 = new ArrayList<ECReaderStat>();
		ECReaderStat readerStat11 = new ECReaderStat();
		readerStat11.setReaderName("reader1");
		readerStats1.add(readerStat11);
		ECReaderStat readerStat12 = new ECReaderStat();
		readerStat12.setReaderName("reader2");
		readerStats1.add(readerStat12);
		names1.setStatBlockList(readerStats1);
		stats1.add(names1);

		ECTagStat sightings1 = new ECTagStat();
		sightings1.setProfile(ReportConstants.ReaderSightingSignalsProfileName);
		ArrayList<ECReaderStat> sightingStats1 = new ArrayList<ECReaderStat>();
		ArrayList<ECSightingStat> sightings11 = new ArrayList<ECSightingStat>();
		sightings11.add(new ECSightingSignalStat("reader1", 1, 10, new Date(0)));
		sightings11.add(new ECSightingSignalStat("reader1", 2, 11, new Date(0)));
		sightings11.add(new ECSightingSignalStat("reader1", 3, 12, new Date(0)));
		sightingStats1.add(new ECReaderStat("reader1", sightings11));
		ArrayList<ECSightingStat> sightings12 = new ArrayList<ECSightingStat>();
		sightings12.add(new ECSightingSignalStat("reader2", 1, 13, new Date(0)));
		sightings12.add(new ECSightingSignalStat("reader2", 2, 14, new Date(0)));
		sightings12.add(new ECSightingSignalStat("reader2", 3, 15, new Date(0)));
		sightingStats1.add(new ECReaderStat("reader2", sightings12));
		sightings1.setStatBlockList(sightingStats1);
		stats1.add(sightings1);

		e1.setECTagStatList(stats1);
		m1.setExtension(e1);
		l.getMember().add(m1);
		ECReportGroupListMember m2 = new ECReportGroupListMember();
		m2.setEpc(new EPC("epc2"));
		ECReportGroupListMemberExtension e2 = new ECReportGroupListMemberExtension();

		e2.setFieldList(new FieldList());
		ECReportMemberField tid2 = new ECReportMemberField();
		tid2.setName("TID");
		tid2.setValue("tidvalue2");
		e2.getFieldList().getField().add(tid2);

		ArrayList<ECTagStat> stats2 = new ArrayList<ECTagStat>();
		ECTagTimestampStat time2 = new ECTagTimestampStat();
		time2.setProfile(ReportConstants.TagTimestampsProfileName);
		time2.setFirstSightingTime(new Date(50000));
		time2.setLastSightingTime(new Date(200000));
		stats2.add(time2);

		ECTagCountStat count2 = new ECTagCountStat();
		count2.setProfile(ReportConstants.TagCountProfileName);
		count2.setCount(4);
		stats2.add(count2);

		ECTagStat names2 = new ECTagStat();
		names2.setProfile(ReportConstants.ReaderNamesProfileName);
		ArrayList<ECReaderStat> readerStats2 = new ArrayList<ECReaderStat>();
		ECReaderStat readerStat21 = new ECReaderStat();
		readerStat21.setReaderName("readerX1");
		readerStats2.add(readerStat21);
		ECReaderStat readerStat22 = new ECReaderStat();
		readerStat22.setReaderName("readerX2");
		readerStats2.add(readerStat22);
		names2.setStatBlockList(readerStats2);
		stats2.add(names2);

		ECTagStat sightings2 = new ECTagStat();
		sightings2.setProfile(ReportConstants.ReaderSightingSignalsProfileName);
		ArrayList<ECReaderStat> sightingStats2 = new ArrayList<ECReaderStat>();
		ArrayList<ECSightingStat> sightings21 = new ArrayList<ECSightingStat>();
		sightings21.add(new ECSightingSignalStat("readerX1", 1, 10, new Date(0)));
		sightings21.add(new ECSightingSignalStat("readerX1", 2, 11, new Date(0)));
		sightings21.add(new ECSightingSignalStat("readerX1", 3, 12, new Date(0)));
		sightingStats2.add(new ECReaderStat("readerX1", sightings21));
		ArrayList<ECSightingStat> sightings22 = new ArrayList<ECSightingStat>();
		sightings22.add(new ECSightingSignalStat("readerX2", 1, 13, new Date(0)));
		sightings22.add(new ECSightingSignalStat("readerX2", 2, 14, new Date(0)));
		sightings22.add(new ECSightingSignalStat("readerX2", 3, 15, new Date(0)));
		sightingStats2.add(new ECReaderStat("readerX2", sightings22));
		ArrayList<ECSightingStat> sightings23 = new ArrayList<ECSightingStat>();
		sightings23.add(new ECSightingSignalStat("readerX2", 1, 13, new Date(0)));
		sightings23.add(new ECSightingSignalStat("readerX2", 2, 14, new Date(0)));
		sightings23.add(new ECSightingSignalStat("readerX2", 3, 15, new Date(0)));
		sightingStats2.add(new ECReaderStat("readerX2", sightings23));
		sightings2.setStatBlockList(sightingStats2);
		stats2.add(sightings2);

		e2.setECTagStatList(stats2);
		m2.setExtension(e2);
		l.getMember().add(m2);
		g.setGroupList(l);
		r.getGroup().add(g);
		report.getReports().getReport().add(r);
		return report;
	}

	private Dummy[] createDummyArray() {
		Dummy contacts[] = new Dummy[3];
		String[][] names = { { "Müller", "Schmidt", "Brinkmann" }, { "Peter", "Hans", "Anna" } };
		int[] ages = { 28, 13, 45 };

		for (int i = 0; i < 3; i++) {
			contacts[i] = new Dummy(names[0][i], names[1][i], ages[i], null, null);
		}
		return contacts;
	}

	private class ListTest {

		private List<Integer> listOne = new ArrayList<Integer>();
		private List<Integer> listTwo = new ArrayList<Integer>();

		public ListTest(int e1, int e2) {
			for (int i = 0; i < e1; i++) {
				listOne.add(Integer.valueOf(i));
			}
			for (int i = 0; i < e2; i++) {
				listTwo.add(Integer.valueOf(i));
			}
		}

		@SuppressWarnings("unused")
		public List<Integer> getListOne() {
			return listOne;
		}

		@SuppressWarnings("unused")
		public List<Integer> getListTwo() {
			return listTwo;
		}
	}
}
