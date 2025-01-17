package org.irods.irods4j.authentication;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.irods.irods4j.api.IRODSApi.RcComm;
import org.irods.irods4j.api.IRODSException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class NativeAuthPlugin extends AuthPlugin {
	
	private static final Logger log = LogManager.getLogger();

	public NativeAuthPlugin() {
		addOperation(AUTH_ESTABLISH_CONTEXT, this::establishContext);
		addOperation(AUTH_CLIENT_AUTH_REQUEST, this::clientRequest);
		addOperation(AUTH_CLIENT_AUTH_RESPONSE, this::clientResponse);
	}

	@Override
	public JsonNode authClientStart(RcComm comm, JsonNode context) {
		var resp = (ObjectNode) context.deepCopy();
		resp.put(AUTH_NEXT_OPERATION, AUTH_CLIENT_AUTH_REQUEST);
		// TODO Why attach the proxy user's name and zone?
		// Seems these should use the client's info.
		resp.put("user_name", comm.proxyUsername);
		resp.put("zone_name", comm.proxyUserZone);
		return resp;
	}

	private JsonNode establishContext(RcComm comm, JsonNode context) throws NoSuchAlgorithmException {
		var resp = (ObjectNode) context.deepCopy();

		var requestResult = ((ObjectNode) context).get("request_result").asText();
		var requestResultSb = new StringBuilder();
		requestResultSb.append(requestResult);

		// Equivalent to std::string::resize.
		// Pads the end of the content with \u0000.
		final var CHALLENGE_LEN = 64;
		requestResultSb.setLength(CHALLENGE_LEN);
		log.debug("requestResultSb string = [{}]", requestResultSb.toString());
		
		// TODO Check for the anonymous user.
		// See plugins/auth/src/native.cpp for details
		// (i.e. native_auth_establish_context.cpp).

		final var MAX_PASSWORD_LEN = 50;
		var passwordSb = new StringBuilder();
		// TODO Enforce the max password length. We need to open an issue for this.
		// The user could enter a long password which gets truncated!
		passwordSb.append(context.get("password").asText());
		passwordSb.setLength(MAX_PASSWORD_LEN);
		// TODO DO NOT LOG PASSWORDS!
//		log.debug("passwordSb string = [{}]", passwordSb.toString());

		// TODO Compute the session signature and store it in the RcComm.
		// See plugins/auth/src/native.cpp for details
		// (i.e. native_auth_establish_context.cpp).

		var md5BufSb = new StringBuilder();
		// [64+1] is the challenge string received by the server.
		// The "+1" is space for the null byte.
		md5BufSb.append(requestResultSb);
		// [50+1] is the deobfuscated user's password.
		// The "+1" is space for the null byte.
		md5BufSb.append(passwordSb);
		log.debug("MD5 string = [{}]", md5BufSb.toString());

		var hasher = MessageDigest.getInstance("md5");
		hasher.update(md5BufSb.toString().getBytes(StandardCharsets.UTF_8));

		// Make sure the byte sequence doesn't end early.
		// Scrub out any errant terminating characters by incrementing
		// their value by 1.
		final var RESPONSE_LEN = 16;
		var digest = hasher.digest();
		for (var i = 0; i < RESPONSE_LEN; ++i) {
			if (0 == digest[i]) {
				++digest[i];
			}
		}

		// DO NOT leak the user's plaintext password!
		resp.remove("password");

		resp.put("digest", Base64.getEncoder().encodeToString(digest));
		resp.put(AUTH_NEXT_OPERATION, AUTH_CLIENT_AUTH_RESPONSE);

		return resp;
	}

	private JsonNode clientRequest(RcComm comm, JsonNode context) throws IOException, IRODSException {
		var req = (ObjectNode) context.deepCopy();
		req.put(AUTH_NEXT_OPERATION, AUTH_AGENT_AUTH_REQUEST);

		var resp = request(comm, req);
		((ObjectNode) resp).put(AUTH_NEXT_OPERATION, AUTH_ESTABLISH_CONTEXT);

		return resp;
	}

	private JsonNode clientResponse(RcComm comm, JsonNode context) throws IOException, IRODSException {
		if (!context.has("digest") || !context.has("user_name") || !context.has("zone_name")) {
			throw new IllegalStateException("Missing digest, user_name, and/or zone_name");
		}

		var req = (ObjectNode) context.deepCopy();
		req.put(AUTH_NEXT_OPERATION, AUTH_AGENT_AUTH_RESPONSE);
		var resp = request(comm, req);

		comm.loggedIn = true;
		((ObjectNode) resp).put(AUTH_NEXT_OPERATION, AUTH_FLOW_COMPLETE);

		return resp;
	}

}
