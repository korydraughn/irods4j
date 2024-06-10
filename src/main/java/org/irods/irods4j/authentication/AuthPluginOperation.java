package org.irods.irods4j.authentication;

import org.irods.irods4j.low_level.api.IRODSApi.RcComm;

import com.fasterxml.jackson.databind.JsonNode;

public interface AuthPluginOperation {
	
	public JsonNode execute(RcComm comm, JsonNode context) throws Exception;

}
