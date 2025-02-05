package org.irods.irods4j.authentication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.irods.irods4j.common.JsonUtil;
import org.irods.irods4j.low_level.api.IRODSException;
import org.irods.irods4j.low_level.api.IRODSApi.RcComm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class AuthManager {
	
	private static final Logger log = LogManager.getLogger();

	public static void authenticateClient(RcComm comm, String authScheme, JsonNode context) throws Exception {
		log.debug(">>> STARTING AUTHENTICATION");

		// TODO Dynamically load plugin based on auth scheme.
		AuthPlugin plugin;
		if ("native".equals(authScheme)) {
			plugin = new NativeAuthPlugin();
		}
		else if ("pam_password".equals(authScheme)) {
			plugin = new PamPasswordAuthPlugin();
		}
		else {
			throw new IllegalArgumentException("Authentication scheme not supported: " + authScheme);
		}

		String nextOp = AuthPlugin.AUTH_CLIENT_START;

		JsonNode req = context.deepCopy();
		ObjectNode mutableReq = (ObjectNode) req;
		mutableReq.put("scheme", authScheme);
		mutableReq.put("next_operation", nextOp);
		
		log.debug("Initial context = {}", JsonUtil.toJsonString(req));

		while (true) {
			JsonNode resp = plugin.execute(comm, nextOp, req);
			log.debug("Server response = {}", JsonUtil.toJsonString(resp));

			if (comm.loggedIn) {
				break;
			}

			if (!resp.has("next_operation")) {
				// TODO Throw exception. IllegalStateException perhaps?
				// C++ throws SYS_INVALID_INPUT_PARAM and reports the following:
				// - "authentication request missing [{}] parameter"
				throw new IRODSException(-130000, "Authentication request missing [next_operation] parameter");
			}

			nextOp = resp.get("next_operation").asText();
			if (nextOp.isEmpty() || "flow_complete".equals(nextOp)) {
				// TODO Throw exception.
				// C++ throws CAT_INVALID_AUTHENTICATION and reports the following:
				// - "authentication flow completed without success"
				throw new IRODSException(-826000, "Authentication flow completed without success");

				// Why does "flow_complete" result in an exception?
			}

			req = resp;
		}
	}

}
