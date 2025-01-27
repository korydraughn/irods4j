package org.irods.irods4j.high_level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Paths;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.irods.irods4j.api.IRODSApi;
import org.irods.irods4j.api.IRODSKeywords;
import org.irods.irods4j.common.JsonUtil;
import org.irods.irods4j.common.XmlUtil;
import org.irods.irods4j.high_level.connection.IRODSConnection;
import org.irods.irods4j.high_level.connection.QualifiedUsername;
import org.irods.irods4j.high_level.io.IRODSDataObjectStream;
import org.irods.irods4j.high_level.vfs.IRODSCollectionIterator;
import org.irods.irods4j.low_level.protocol.packing_instructions.DataObjInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.DataObjInp_PI.OpenFlags;
import org.irods.irods4j.low_level.protocol.packing_instructions.KeyValPair_PI;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TestIRODSCollectionIterator {

	static final Logger log = LogManager.getLogger();

	static String host = "localhost";
	static int port = 1247;
	static String zone = "tempZone";
	static String username = "rods";
	static String password = "rods";
	static IRODSConnection conn;

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

		XmlUtil.disablePrettyPrinting();
		JsonUtil.disablePrettyPrinting();
	}

	@Test
	void testIteratingOverCollectionHoldingAllHomeCollections() throws Exception {
		var paths = new ArrayList<String>();
		var collection = Paths.get("/", zone, "home").toString();
		try (var iterator = new IRODSCollectionIterator(conn.getRcComm(), collection)) {
			for (var e : iterator) {
				paths.add(e.path());
			}
		}
		assertFalse(paths.isEmpty());
	}

	@Test
	void testIteratingOverAnEmptyCollection() throws Exception {
		var paths = new ArrayList<String>();
		var collection = Paths.get("/", zone, "home", "public").toString();
		try (var iterator = new IRODSCollectionIterator(conn.getRcComm(), collection)) {
			for (var e : iterator) {
				paths.add(e.path());
			}
		}
		assertTrue(paths.isEmpty());
	}

	@Test
	void testConstructingAnIteratorFromADataObjectPathResultsInANullIterator() throws Exception {
		var dataObjectName = "testIteratingUsingADataObjectPathResultsInAnException";
		var path = Paths.get("/", zone, "home", username, dataObjectName).toString();

		try {
			// Create the data object.
			try (var dataObject = new IRODSDataObjectStream()) {
				dataObject.open(conn.getRcComm(), path, OpenFlags.O_CREAT | OpenFlags.O_WRONLY);
			}

			var collIterator = new IRODSCollectionIterator(conn.getRcComm(), path);
			var iter = collIterator.iterator();
			assertNotNull(iter);
			assertFalse(iter.hasNext());
		} finally {
			// Remove the data object.
			var input = new DataObjInp_PI();
			input.objPath = path;
			input.KeyValPair_PI = new KeyValPair_PI();
			input.KeyValPair_PI.ssLen = 1;
			input.KeyValPair_PI.keyWord = new ArrayList<>();
			input.KeyValPair_PI.svalue = new ArrayList<>();
			input.KeyValPair_PI.keyWord.add(IRODSKeywords.FORCE_FLAG);
			input.KeyValPair_PI.svalue.add("");
			assertEquals(IRODSApi.rcDataObjUnlink(conn.getRcComm(), input), 0);
		}
	}

}
