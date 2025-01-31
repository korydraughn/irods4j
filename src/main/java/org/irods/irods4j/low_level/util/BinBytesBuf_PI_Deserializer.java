package org.irods.irods4j.low_level.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.irods.irods4j.low_level.protocol.packing_instructions.BinBytesBuf_PI;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

public class BinBytesBuf_PI_Deserializer extends JsonDeserializer<BinBytesBuf_PI> {

	@Override
	public BinBytesBuf_PI deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
		var v = new BinBytesBuf_PI();
		var node = ctxt.readTree(p);

		var targetNode = node.at("/buflen");
		if (targetNode.isMissingNode()) {
			throw new IOException("BinBytesBuf_PI deserialization error: Missing [buflen]");
		}
		var p1 = targetNode.traverse();
		p1.nextToken();
		v.buflen = ctxt.readValue(p1, Integer.class);

		targetNode = node.at("/buf");
		if (!targetNode.isMissingNode()) {
			p1 = targetNode.traverse();
			p1.nextToken();
			var decodedBytes = Base64.getDecoder().decode(ctxt.readValue(p1, String.class));
			v.buf = new String(decodedBytes, StandardCharsets.UTF_8);
		}

		return v;
	}

}
