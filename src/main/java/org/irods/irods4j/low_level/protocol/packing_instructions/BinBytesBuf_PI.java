package org.irods.irods4j.low_level.protocol.packing_instructions;

import org.irods.irods4j.low_level.util.BinBytesBuf_PI_Deserializer;
import org.irods.irods4j.low_level.util.BinBytesBuf_PI_Serializer;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(using = BinBytesBuf_PI_Serializer.class)
@JsonDeserialize(using = BinBytesBuf_PI_Deserializer.class)
public class BinBytesBuf_PI {

	public int buflen;
	public String buf;
	
}
