package org.irods.irods4j.authentication;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.irods.irods4j.common.JsonUtil;
import org.irods.irods4j.low_level.api.IRODSApi.RcComm;
import org.irods.irods4j.low_level.api.IRODSException;

import java.io.IOException;

public class PamInteractiveAuthPlugin extends AuthPlugin {

	private static final Logger log = LogManager.getLogger();

	private static final String PERFORM_RUNNING = "running";
	private static final String PERFORM_READY = "ready";
	private static final String PERFORM_WAITING = "waiting";
	private static final String PERFORM_WAITING_PW = "waiting_pw";
	private static final String PERFORM_RESPONSE = "response";
	private static final String PERFORM_NEXT = "next";
	private static final String PERFORM_ERROR = "error";
	private static final String PERFORM_TIMEOUT = "timeout";
	private static final String PERFORM_AUTHENTICATED = "authenticated";
	private static final String PERFORM_NOT_AUTHENTICATED = "non_authenticated";
	private static final String PERFORM_NATIVE_AUTH = "perform_native_auth";

	public PamInteractiveAuthPlugin() {
		addOperation(AUTH_CLIENT_AUTH_REQUEST, this::clientRequest);
		addOperation(AUTH_CLIENT_AUTH_RESPONSE, this::clientResponse);
		addOperation(PERFORM_RUNNING, this::stepGeneric); // C++: step_client_running
		addOperation(PERFORM_READY, this::stepGeneric); // C++: step_client_ready
		addOperation(PERFORM_NEXT, this::stepClientNext);
		addOperation(PERFORM_RESPONSE, this::stepGeneric); // C++: step_client_response
		addOperation(PERFORM_WAITING, this::stepWaiting);
		addOperation(PERFORM_WAITING_PW, this::stepWaitingPw);
		addOperation(PERFORM_ERROR, this::stepError);
		addOperation(PERFORM_TIMEOUT, this::stepTimeout);
		addOperation(PERFORM_AUTHENTICATED, this::stepAuthenticated);
		addOperation(PERFORM_NOT_AUTHENTICATED, this::stepNotAuthenticated);
		addOperation(PERFORM_NATIVE_AUTH, this::performNativeAuth);
	}

	@Override
	public JsonNode authClientStart(RcComm comm, JsonNode context) {
		var resp = (ObjectNode) context.deepCopy();
		resp.put(AUTH_NEXT_OPERATION, AUTH_CLIENT_AUTH_REQUEST);

		// Initialize the persistence state of the PAM stack.
		resp.put("pdirty", false);
		resp.set("pstate", JsonUtil.getJsonMapper().createObjectNode());

		// The proxy user's information is used for authentication because it covers
		// all cases for connecting to iRODS (i.e. proxied and non-proxied connections).
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

		// Unlike the C++ implementation, this library requires the user to connect
		// using a secure channel. It is the user's responsibility to make sure the
		// communication is secure before authenticating via PAM.
		if (!comm.secure) {
			throw new IllegalStateException("SSL/TLS is required for PAM authentication");
		}

		var resp = (ObjectNode) request(comm, req);
		resp.put(AUTH_NEXT_OPERATION, AUTH_CLIENT_AUTH_RESPONSE);
		return resp;
	}

	private JsonNode clientResponse(RcComm rcComm, JsonNode context) throws IRODSException, IOException {
		if (!context.has("user_name")) {
			throw new IllegalStateException("Missing property: user_name");
		}

		if (!context.has("zone_name")) {
			throw new IllegalStateException("Missing property: zone_name");
		}

		var resp = (ObjectNode) context.deepCopy();
		resp.put(AUTH_NEXT_OPERATION, AUTH_AGENT_AUTH_RESPONSE);
		return request(rcComm, resp);
	}

	private JsonNode stepClientNext(RcComm rcComm, JsonNode context) throws IRODSException, IOException {
		var resp = (ObjectNode) context.deepCopy();
		var prompt = resp.get("msg").get("prompt");
		if (prompt.isTextual()) {
			System.out.println(prompt.asText());
		}
		patchState(resp);
		resp.put(AUTH_NEXT_OPERATION, AUTH_AGENT_AUTH_RESPONSE);
		return request(rcComm, resp);
	}

	private JsonNode stepWaiting(RcComm rcComm, JsonNode context) throws IRODSException, IOException {
		var resp = (ObjectNode) context.deepCopy();

		if (retrieveEntry(resp)) {
			resp.put(AUTH_NEXT_OPERATION, AUTH_AGENT_AUTH_RESPONSE);
			patchState(resp);
			return request(rcComm, resp);
		}

		var prompt = resp.get("msg").get("prompt");
		var defaultValue = getDefaultValue(resp);
		if (defaultValue.is) // TODO

		return request(rcComm, resp);
	}

	private JsonNode stepWaitingPw(RcComm rcComm, JsonNode context) throws IRODSException, IOException {
		var resp = (ObjectNode) context.deepCopy();
		patchState(resp);
		resp.put(AUTH_NEXT_OPERATION, AUTH_AGENT_AUTH_RESPONSE);
		return request(rcComm, resp);
	}

	private JsonNode stepError(RcComm rcComm, JsonNode context) throws IRODSException, IOException {
		System.out.println("error");
		var resp = (ObjectNode) context.deepCopy();
		resp.put(AUTH_NEXT_OPERATION, AUTH_FLOW_COMPLETE);
		rcComm.loggedIn = false;
		return resp;
	}

	private JsonNode stepTimeout(RcComm rcComm, JsonNode context) throws IRODSException, IOException {
		System.out.println("timeout");
		var resp = (ObjectNode) context.deepCopy();
		resp.put(AUTH_NEXT_OPERATION, AUTH_FLOW_COMPLETE);
		rcComm.loggedIn = false;
		return resp;
	}

	private JsonNode stepAuthenticated(RcComm rcComm, JsonNode context) throws IRODSException, IOException {
		var resp = (ObjectNode) context.deepCopy();
		resp.put(AUTH_NEXT_OPERATION, PERFORM_NATIVE_AUTH);
		return resp;
	}

	private JsonNode stepNotAuthenticated(RcComm rcComm, JsonNode context) throws IRODSException, IOException {
		var resp = (ObjectNode) context.deepCopy();
		resp.put(AUTH_NEXT_OPERATION, AUTH_FLOW_COMPLETE);
		rcComm.loggedIn = false;
		return resp;
	}

	private JsonNode stepGeneric(RcComm rcComm, JsonNode context) throws IRODSException, IOException {
		var resp = (ObjectNode) context.deepCopy();
		patchState(resp);
		resp.put(AUTH_NEXT_OPERATION, AUTH_AGENT_AUTH_RESPONSE);
		return request(rcComm, resp);
	}

	private JsonNode performNativeAuth(RcComm comm, JsonNode context) throws Exception {
		var resp = (ObjectNode) context.deepCopy();

		// Remove the PAM password from the payload so it's not sent over the network.
		resp.remove(AUTH_PASSWORD_KEY);

		// Authenticate using the native authentication and the server generated
		// password mapped to "request_result".
		var input = JsonUtil.getJsonMapper().createObjectNode();
		input.put("password", resp.get("request_result").asText());
		AuthManager.authenticateClient(comm, "native", input);

		// If everything completed successfully, the flow is complete and we can
		// consider the user to be logged in. The native auth flow was run and so
		// we trust the result.
		resp.put(AUTH_NEXT_OPERATION, AUTH_FLOW_COMPLETE);

		return resp;
	}

	private void patchState(JsonNode state) {
		if (!state.has("patch")) {
			return;
		}

		ObjectNode patch = state.get("patch").deepCopy();
		patch.forEach(node -> {
			var op = node.get("op");
			var opStr = null != op ? op.asText("") : "";
			if ("add".equals(opStr) || "replace".equals(opStr)) {
				if (!node.has("value")) {
					var resp = state.get("resp");
					var value = (null != resp) ? resp.asText("") : "";
					((ObjectNode) node).put("value", value);
				}
			}
		});

		var node = (ObjectNode) state;
		//node.put("pstate", ) // TODO req["pstate"].patch(patch);
		node.put("pdirty", true);
		((ObjectNode) node.get("msg")).remove("patch");
	}

	private String getDefaultValue(JsonNode request) {
		var defaultPath = request.get("msg").get("default_path");
		if (null == defaultPath) {
			return "";
		}

		var defaultValue = request.get("pstate").at(defaultPath.asText());
		if (!defaultValue.isMissingNode()) {
			return defaultValue.asText();
		}

		return "";
	}

	private boolean retrieveEntry(JsonNode request) {
		var msg = request.get("msg");
		if (null != msg && msg.has("retrieve")) {
			var retrPath = msg.get("retrieve");
			if (null != retrPath) {
				var pstate = request.get("pstate").at(retrPath.asText());
				if (!pstate.isMissingNode()) {
					((ObjectNode) request).put("resp", pstate.asText());
					return true;
				}
			}

			((ObjectNode) request).put("resp", "");
			return true;
		}

		return false;
	}

	private String getStringValue(ObjectNode node, String key, String defaultValue) {
		var tmp = node.get(key);
		return (null != tmp) ? tmp.asText(defaultValue) : defaultValue;
	}

}
