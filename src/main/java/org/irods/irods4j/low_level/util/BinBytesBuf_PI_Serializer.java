package org.irods.irods4j.low_level.util;

import java.io.IOException;
import java.util.Base64;

import org.irods.irods4j.low_level.protocol.packing_instructions.BinBytesBuf_PI;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class BinBytesBuf_PI_Serializer extends JsonSerializer<BinBytesBuf_PI> {

	@Override
	public void serialize(BinBytesBuf_PI value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		gen.writeStartObject();
		gen.writeNumberField("buflen", value.buflen);
		gen.writeStringField("buf", Base64.getEncoder().encodeToString(value.buf.getBytes()));
		gen.writeEndObject();
	}

}
