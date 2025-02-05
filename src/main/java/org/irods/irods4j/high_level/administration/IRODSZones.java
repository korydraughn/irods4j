package org.irods.irods4j.high_level.administration;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.irods.irods4j.common.JsonUtil;
import org.irods.irods4j.common.Reference;
import org.irods.irods4j.low_level.api.IRODSApi;
import org.irods.irods4j.low_level.api.IRODSException;
import org.irods.irods4j.low_level.api.IRODSApi.RcComm;
import org.irods.irods4j.low_level.protocol.packing_instructions.GeneralAdminInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.Genquery2Input_PI;

import com.fasterxml.jackson.core.type.TypeReference;

public class IRODSZones {

	public static enum ZoneCollectionAcl {
		NULL, READ
	}

	public static class ZoneProperty {
	}

	public static final class ZoneNameProperty extends ZoneProperty {
		public String value;
	}

	public static final class ConnectionInfoProperty extends ZoneProperty {
		public String value;
	}

	public static final class CommentProperty extends ZoneProperty {
		public String value;
	}

	public static final class ZoneCollectionAclProperty extends ZoneProperty {
		public ZoneCollectionAcl acl;
		public String name;
	}

	public static enum ZoneType {
		LOCAL, REMOTE
	}

	public static final class ZoneInfo {
		public String name;
		public String connectionInfo;
		public String comment;
		public int id;
		public ZoneType type;
	}

	public static final class ZoneOptions {
		public String connectionInfo;
		public String comment;
	}

	public static void addZone(RcComm comm, String zoneName, ZoneOptions options) throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == zoneName || zoneName.isEmpty()) {
			throw new IllegalArgumentException("Zone name is null or empty");
		}

		GeneralAdminInp_PI input = new GeneralAdminInp_PI();
		input.arg0 = "add";
		input.arg1 = "zone";
		input.arg2 = zoneName;
		input.arg3 = "remote";
		input.arg4 = (null == options.connectionInfo) ? "" : options.connectionInfo;
		input.arg5 = (null == options.comment) ? "" : options.comment;

		int ec = IRODSApi.rcGeneralAdmin(comm, input);
		if (ec < 0) {
			throw new IRODSException(ec, "rcGeneralAdmin error");
		}
	}

	public static void removeZone(RcComm comm, String zoneName) throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == zoneName || zoneName.isEmpty()) {
			throw new IllegalArgumentException("Zone name is null or empty");
		}

		GeneralAdminInp_PI input = new GeneralAdminInp_PI();
		input.arg0 = "rm";
		input.arg1 = "zone";
		input.arg2 = zoneName;

		int ec = IRODSApi.rcGeneralAdmin(comm, input);
		if (ec < 0) {
			throw new IRODSException(ec, "rcGeneralAdmin error");
		}
	}

	public static void modifyZone(RcComm comm, String zoneName, ZoneProperty property)
			throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == zoneName || zoneName.isEmpty()) {
			throw new IllegalArgumentException("Zone name is null or empty");
		}

		if (null == property) {
			throw new IllegalArgumentException("Zone property is null");
		}

		if (property instanceof ZoneNameProperty) {
			ZoneNameProperty p = (ZoneNameProperty) property;
			GeneralAdminInp_PI input = new GeneralAdminInp_PI();
			input.arg0 = "modify";

			if (comm.clientUserZone.equals(zoneName)) {
				input.arg1 = "localzonename";
				input.arg2 = zoneName;
				input.arg3 = p.value;
			} else {
				input.arg1 = "zone";
				input.arg2 = zoneName;
				input.arg3 = "name";
				input.arg4 = p.value;
			}

			int ec = IRODSApi.rcGeneralAdmin(comm, input);
			if (ec < 0) {
				throw new IRODSException(ec, "rcGeneralAdmin error");
			}
		} else if (property instanceof ConnectionInfoProperty) {
			ConnectionInfoProperty p = (ConnectionInfoProperty) property;
			GeneralAdminInp_PI input = new GeneralAdminInp_PI();
			input.arg0 = "modify";
			input.arg1 = "zone";
			input.arg2 = zoneName;
			input.arg3 = "conn";
			input.arg4 = p.value;

			int ec = IRODSApi.rcGeneralAdmin(comm, input);
			if (ec < 0) {
				throw new IRODSException(ec, "rcGeneralAdmin error");
			}
		} else if (property instanceof CommentProperty) {
			CommentProperty p = (CommentProperty) property;
			GeneralAdminInp_PI input = new GeneralAdminInp_PI();
			input.arg0 = "modify";
			input.arg1 = "zone";
			input.arg2 = zoneName;
			input.arg3 = "comment";
			input.arg4 = p.value;

			int ec = IRODSApi.rcGeneralAdmin(comm, input);
			if (ec < 0) {
				throw new IRODSException(ec, "rcGeneralAdmin error");
			}
		} else if (property instanceof ZoneCollectionAclProperty) {
			ZoneCollectionAclProperty p = (ZoneCollectionAclProperty) property;
			String perm = ZoneCollectionAcl.NULL == p.acl ? "null" : "read";
			String zoneColl = "/" + zoneName;

			GeneralAdminInp_PI input = new GeneralAdminInp_PI();
			input.arg0 = "modify";
			input.arg1 = "zonecollacl";
			input.arg2 = perm;
			input.arg3 = p.name;
			input.arg4 = zoneColl;

			int ec = IRODSApi.rcGeneralAdmin(comm, input);
			if (ec < 0) {
				throw new IRODSException(ec, "rcGeneralAdmin error");
			}
		} else {
			throw new IllegalArgumentException("Zone property not supported");
		}
	}

	public static boolean zoneExists(RcComm comm, String zoneName) throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == zoneName || zoneName.isEmpty()) {
			throw new IllegalArgumentException("Zone name is null or empty");
		}

		Genquery2Input_PI input = new Genquery2Input_PI();
		input.query_string = String.format("select ZONE_NAME where ZONE_NAME = '%s'", zoneName);
//		input.zone = comm.clientUserZone;

		Reference<String> output = new Reference<String>();
		int ec = IRODSApi.rcGenQuery2(comm, input, output);
		if (ec < 0) {
			throw new IRODSException(ec, "rcGenQuery2 error");
		}

		TypeReference<List<List<String>>> typeRef = new TypeReference<List<List<String>>>() {
		};
		List<List<String>> rows = JsonUtil.fromJsonString(output.value, typeRef);

		return !rows.isEmpty();
	}

	public static Optional<ZoneInfo> zoneInfo(RcComm comm, String zoneName) throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == zoneName || zoneName.isEmpty()) {
			throw new IllegalArgumentException("Zone name is null or empty");
		}

		Genquery2Input_PI input = new Genquery2Input_PI();
		input.query_string = String
				.format("select ZONE_ID, ZONE_TYPE, ZONE_CONNECTION, ZONE_COMMENT where ZONE_NAME = '%s'", zoneName);
//		input.zone = comm.clientUserZone;

		Reference<String> output = new Reference<String>();
		int ec = IRODSApi.rcGenQuery2(comm, input, output);
		if (ec < 0) {
			throw new IRODSException(ec, "rcGenQuery2 error");
		}

		TypeReference<List<List<String>>> typeRef = new TypeReference<List<List<String>>>() {
		};
		List<List<String>> rows = JsonUtil.fromJsonString(output.value, typeRef);
		if (rows.isEmpty()) {
			return Optional.empty();
		}

		List<String> row = rows.get(0);
		ZoneInfo info = new ZoneInfo();
		info.name = zoneName;
		info.id = Integer.parseInt(row.get(0));
		info.type = "local".equals(row.get(1)) ? ZoneType.LOCAL : ZoneType.REMOTE;
		info.connectionInfo = row.get(2);
		info.comment = row.get(3);

		return Optional.of(info);
	}

}
