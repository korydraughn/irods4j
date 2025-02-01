package org.irods.irods4j.high_level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.irods.irods4j.common.JsonUtil;
import org.irods.irods4j.common.XmlUtil;
import org.irods.irods4j.high_level.administration.IRODSUsers.User;
import org.irods.irods4j.high_level.catalog.IRODSQuery;
import org.irods.irods4j.high_level.catalog.IRODSQuery.GenQuery1QueryArgs;
import org.irods.irods4j.high_level.connection.IRODSConnection;
import org.irods.irods4j.high_level.connection.QualifiedUsername;
import org.irods.irods4j.low_level.api.GenQuery1Columns;
import org.irods.irods4j.low_level.api.GenQuery1SortOptions;
import org.irods.irods4j.low_level.api.IRODSException;
import org.irods.irods4j.low_level.api.IRODSKeywords;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TestIRODSQuery {

	static final Logger log = LogManager.getLogger();

	static String host = "localhost";
	static int port = 1247;
	static String zone = "tempZone";
	static String username = "rods";
	static String password = "rods";
	static IRODSConnection conn;

	static User rodsuser = new User("TestIRODSUsers_alice", Optional.of(zone));

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		XmlUtil.enablePrettyPrinting();
		JsonUtil.enablePrettyPrinting();

		conn = new IRODSConnection();
		conn.connect(host, port, new QualifiedUsername(username, zone));
		conn.authenticate("native", password);
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
		conn.disconnect();
	}

	@Test
	void testIRODSQueryCorrectlyHandlesNonEmptyResultsets() throws Exception {
		var rows = IRODSQuery.executeGenQuery2(conn.getRcComm(), "select COLL_NAME");
		assertNotNull(rows);
		assertFalse(rows.isEmpty());
	}

	@Test
	void testIRODSQueryCorrectlyHandlesEmptyResultsets() throws Exception {
		var rows = IRODSQuery.executeGenQuery2(conn.getRcComm(),
				"select COLL_NAME where COLL_NAME = 'produce_empty_resultset'");
		assertNotNull(rows);
		assertTrue(rows.isEmpty());
	}

	@Test
	void testIRODSQueryReturnsColumnMappings() throws Exception {
		var mappings = IRODSQuery.getColumnMappings(conn.getRcComm());
		assertNotNull(mappings);
		assertTrue(mappings.containsKey("DATA_NAME"));

		var column = mappings.get("DATA_NAME");
		assertEquals(column.get("R_DATA_MAIN"), "data_name");
	}

	@Test
	void testIRODSQueryReturnsGeneratedSQL() throws Exception {
		var sql = IRODSQuery.getGeneratedSQL(conn.getRcComm(), "select COLL_NAME");
		assertNotNull(sql);
		assertTrue(sql.contains("select "));
		assertTrue(sql.contains(" R_COLL_MAIN t0 "));
	}

	@Test
	void testIRODSQueryHandlesSpecificQueries() throws Exception {
		var bindArgs = Arrays.asList(username);

		IRODSQuery.executeSpecificQuery(conn.getRcComm(), "listGroupsForUser", bindArgs, row -> {
			// On a fresh iRODS install, we expect the "rods" user to be a member of the
			// "public" group only.
			assertEquals(row.size(), 2);

			var sb = new StringBuilder();
			sb.append('[');
			for (var c = 0; c < row.size(); ++c) {
				if (c > 0) {
					sb.append(", ");
				}
				sb.append(row.get(c));
			}
			sb.append(']');

			log.debug(sb.toString());

			assertTrue(sb.toString().contains(", public"));

			// Continue iterating through the results, one row at a time. If false is
			// returned, iteration stops and the rest of the results are discarded.
			return true;
		});
	}

	@Test
	void testIRODSQuerySupportsGenQuery1() throws IOException, IRODSException {
		var input = new GenQuery1QueryArgs();

		// select COLL_NAME, COLL_ACCESS_USER_ID, COLL_ACCESS_NAME ...
		input.addColumnToSelectClause(GenQuery1Columns.COL_COLL_NAME, GenQuery1SortOptions.ORDER_BY_DESC);
		input.addColumnToSelectClause(GenQuery1Columns.COL_COLL_ACCESS_USER_ID);
		input.addColumnToSelectClause(GenQuery1Columns.COL_COLL_ACCESS_NAME);

		// where COLL_NAME like '/%' and COLL_ACCESS_NAME = 'own'
		input.addConditionToWhereClause(GenQuery1Columns.COL_COLL_NAME, "like '/%'");
		input.addConditionToWhereClause(GenQuery1Columns.COL_COLL_ACCESS_NAME, "= 'own'");

		// Execute the query in tempZone.
		input.addApiOption(IRODSKeywords.ZONE, zone);

		var output = new StringBuilder();

		IRODSQuery.executeGenQuery1(conn.getRcComm(), input, row -> {
			// Each row should contain 3 columns.
			assertEquals(row.size(), 3);

			output.append('[');
			for (var c = 0; c < row.size(); ++c) {
				if (c > 0) {
					output.append(", ");
				}
				output.append(row.get(c));
			}
			output.append("]\n");

			// Continue iterating through the results, one row at a time. If false is
			// returned, iteration stops and the rest of the results are discarded.
			return true;
		});

		var text = output.toString();
		log.debug("\n" + text);
		assertTrue(text.contains("/" + zone + "/"));
	}

}
