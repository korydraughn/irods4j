package org.irods.irods4j.low_level.protocol.packing_instructions;

public class SwitchUserInp_PI {
	
	public static final String KW_SWITCH_PROXY_USER = "switch_proxy_user";
	public static final String KW_CLOSE_OPEN_REPLICAS = "close_open_replicas";
	public static final String KW_KEEP_SVR_TO_SVR_CONNECTIONS = "keep_svr_to_svr_connections";
	
	public String username;
	public String zone;
	public KeyValPair_PI KeyValPair_PI;

}
