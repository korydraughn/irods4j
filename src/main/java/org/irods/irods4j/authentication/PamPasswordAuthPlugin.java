package org.irods.irods4j.authentication;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.irods.irods4j.api.IRODSApi.RcComm;
import org.irods.irods4j.api.IRODSException;

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
		resp.put("user_name", comm.getProxyUsername());
		resp.put("zone_name", comm.getProxyUserZone());
		// Map the user's password to the correct property name.
		resp.put(AuthConstants.AUTH_PASSWORD_KEY, context.get("password").asText());
		// DO NOT leak the user's password!
		resp.remove("password");
		return resp;
	}

	private JsonNode clientRequest(RcComm comm, JsonNode context) throws IOException, IRODSException {
		var req = (ObjectNode) context.deepCopy();
		req.put(AUTH_NEXT_OPERATION, AUTH_AGENT_AUTH_REQUEST);
		
		// TODO Enable TLS for a secure connection.

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

	private JsonNode performNativeAuth(RcComm comm, JsonNode context) throws IOException, IRODSException {
		// TODO Update this code based on the C++. It has not been updated yet.
		
		var req = (ObjectNode) context.deepCopy();
		req.put(AUTH_NEXT_OPERATION, AUTH_AGENT_AUTH_RESPONSE);
		var resp = request(comm, req);

		comm.setLoggedInToTrue();
		((ObjectNode) resp).put(AUTH_NEXT_OPERATION, AUTH_FLOW_COMPLETE);

		return resp;
	}

}
