package org.irods.irods4j.low_level.protocol.packing_instructions;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

public class Genquery2Input_PI {

	public String query_string;

	// Instructs the serializer to not include null fields.
	// This aligns with the C packing instruction implementation
	// and keeps the users of the library from having to define
	// a zone, which is exactly what we want.
	@JsonInclude(Include.NON_NULL)
	public String zone;

	public int sql_only;
	public int column_mappings;

}
