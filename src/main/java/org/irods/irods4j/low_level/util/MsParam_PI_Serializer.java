package org.irods.irods4j.low_level.util;

import java.io.IOException;

import org.irods.irods4j.low_level.protocol.packing_instructions.MsParam_PI;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class MsParam_PI_Serializer extends JsonSerializer<MsParam_PI>{

	@Override
	public void serialize(MsParam_PI value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		gen.writeStartObject();
		gen.writeStringField("label", value.label);
		gen.writeStringField("type", value.type);
		gen.writeFieldName(value.type);
		gen.writePOJO(value.inOutStruct);
		gen.writeFieldName("BinBytesBuf_PI");
		gen.writePOJO(value.BinBytesBuf_PI);
		gen.writeEndObject();
	}

}
