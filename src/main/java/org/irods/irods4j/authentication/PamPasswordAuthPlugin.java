package org.irods.irods4j.authentication;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.irods.irods4j.api.IRODSApi.RcComm;
import org.irods.irods4j.api.IRODSException;
import org.irods.irods4j.common.JsonUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class PamPasswordAuthPlugin extends AuthPlugin {

	private static final Logger log = LogManager.getLogger();

	private static final String PERFORM_NATIVE_AUTH = "perform_native_auth";

	public PamPasswordAuthPlugin() {
		addOperation(AUTH_CLIENT_AUTH_REQUEST, this::clientRequest);
		addOperation(PERFORM_NATIVE_AUTH, this::performNativeAuth);
	}

	@Override
	public JsonNode authClientStart(RcComm comm, JsonNode context) {
		var resp = (ObjectNode) context.deepCopy();
		resp.put(AUTH_NEXT_OPERATION, AUTH_CLIENT_AUTH_REQUEST);
		// TODO Why attach the proxy user's name and zone?
		// Seems these should use the client's info.
		resp.put("user_name", comm.proxyUsername);
		resp.put("zone_name", comm.proxyUserZone);
		// Map the user's password to the correct property name.
		resp.put(AUTH_PASSWORD_KEY, context.get("password").asText());
		// DO NOT leak the user's password!
		resp.remove("password");
		return resp;
	}

	private JsonNode clientRequest(RcComm comm, JsonNode context) throws IOException, IRODSException {
		var req = (ObjectNode) context.deepCopy();
		req.put(AUTH_NEXT_OPERATION, AUTH_AGENT_AUTH_REQUEST);

		// TODO Enable TLS for a secure connection.
		// This library assumes TLS is enabled at this point.
		// The C++ calls sslStart() here if SSL isn't enabled. sslStart()
		// ONLY establishes a secure connection. The encryption options are
		// never set by it.
		//
		// See sslSockComm.cpp#L30 for details.

		var resp = request(comm, req);
		((ObjectNode) resp).put(AUTH_NEXT_OPERATION, PERFORM_NATIVE_AUTH);

		if (!resp.has("request_result")) {
			throw new IllegalStateException("Missing property: request_result");
		}

		// TODO The C++ implementation obfuscates "request_result" and stores
		// it in the user's .irodsA file for native authentication to pick up.
		// We need to determine whether that's necessary here.
		//
		// The question is ... is "request_result" an obfuscated value?

		return resp;
	}

	private JsonNode performNativeAuth(RcComm comm, JsonNode context) throws Exception {
		var resp = (ObjectNode) context.deepCopy();

		// Remove the PAM password from the payload so it's not sent over the network.
		resp.remove(AUTH_PASSWORD_KEY);
		
		// Authenticate using the native authentication and the server generated
		// password mapped to "request_result".
		var input = JsonUtil.getJsonMapper().createObjectNode();
		input.put("password", resp.get("request_result").asText());
//		input.put(AuthPlugin.AUTH_TTL_KEY, "0");
		AuthManager.authenticateClient(comm, "native", input);

		// If everything completed successfully, the flow is complete and we can
		// consider the user to be logged in. The native auth flow was run and so
		// we trust the result.
		resp.put(AUTH_NEXT_OPERATION, AUTH_FLOW_COMPLETE);

		return resp;
	}

}
