package org.irods.irods4j.common;

import java.io.IOException;

import org.irods.irods4j.low_level.util.NullSerializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.ser.XmlSerializerProvider;
import com.fasterxml.jackson.dataformat.xml.util.XmlRootNameLookup;

public class XmlUtil {

	private static XmlMapper xm = new XmlMapper();

	static {
		XmlSerializerProvider provider = new XmlSerializerProvider(new XmlRootNameLookup());
		provider.setNullValueSerializer(new NullSerializer());
		xm.setSerializerProvider(provider);
	}

	public static XmlMapper setXmlMapper(XmlMapper other) {
		XmlMapper old = xm;
		xm = other;
		return old;
	}

	public static XmlMapper getXmlMapper() {
		return xm;
	}

	public static void enablePrettyPrinting() {
		xm.enable(SerializationFeature.INDENT_OUTPUT);
	}

	public static void disablePrettyPrinting() {
		xm.disable(SerializationFeature.INDENT_OUTPUT);
	}

	public static String toXmlString(Object object) throws JsonProcessingException {
		return xm.writeValueAsString(object);
	}

	public static <T> T fromXmlString(String data, Class<T> clazz)
			throws JsonMappingException, JsonProcessingException {
		return xm.readValue(data, clazz);
	}

	public static <T> T fromXmlString(String data, TypeReference<T> typeRef)
			throws JsonMappingException, JsonProcessingException {
		return xm.readValue(data, typeRef);
	}

	public static <T> T fromBytes(byte[] data, Class<T> clazz) throws IOException {
		return xm.readValue(data, clazz);
	}

	public static <T> T fromBytes(byte[] data, TypeReference<T> typeRef) throws IOException {
		return xm.readValue(data, typeRef);
	}

}
