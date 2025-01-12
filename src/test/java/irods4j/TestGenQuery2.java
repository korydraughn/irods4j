package irods4j;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.irods.irods4j.api.API;
import org.irods.irods4j.api.IRODSException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestGenQuery2 {
	
	private static final Logger log = LogManager.getLogger();

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
	}

	@BeforeEach
	void setUp() throws Exception {
	}

	@AfterEach
	void tearDown() throws Exception {
	}

	@Test
	void testRcGenQuery2() throws IOException, IRODSException {
		var host = "localhost";
		var port = 1247;
		var zone = "tempZone";
		var username = "rods";
		var password = "rods";
		
		var comm = assertDoesNotThrow(() -> API.rcConnect(host, port, zone, username));
		assertNotNull(comm);
		assertDoesNotThrow(() -> API.authenticate(comm, "native", password));

		var sqlOnly = false;
		var columnMappings = false;
		var results = API.rcGenQuery2(comm, "select COLL_NAME", Optional.empty(), sqlOnly, columnMappings);
		assertNotNull(results);
		log.info("results = {}", results);

		assertDoesNotThrow(() -> API.rcDisconnect(comm));
	}

}
