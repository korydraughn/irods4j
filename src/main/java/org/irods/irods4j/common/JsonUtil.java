package org.irods.irods4j.common;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class JsonUtil {

	private static ObjectMapper om = new ObjectMapper();

	public static ObjectMapper setJsonMapper(ObjectMapper other) {
		var old = om;
		om = other;
		return old;
	}

	public static void enablePrettyPrinting() {
		om.enable(SerializationFeature.INDENT_OUTPUT);
	}

	public static void disablePrettyPrinting() {
		om.disable(SerializationFeature.INDENT_OUTPUT);
	}

	public static String toJsonString(Object o) throws JsonProcessingException {
		return om.writeValueAsString(o);
	}

	public static <T> T fromJsonString(String data, Class<T> clazz)
			throws JsonMappingException, JsonProcessingException {
		return om.readValue(data, clazz);
	}

	public static <T> T fromBytes(byte[] data, Class<T> clazz) throws IOException {
		return om.readValue(data, clazz);
	}

}
