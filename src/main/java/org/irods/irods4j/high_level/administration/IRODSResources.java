package org.irods.irods4j.high_level.administration;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.irods.irods4j.api.IRODSApi;
import org.irods.irods4j.api.IRODSApi.RcComm;
import org.irods.irods4j.api.IRODSException;
import org.irods.irods4j.common.JsonUtil;
import org.irods.irods4j.common.Reference;
import org.irods.irods4j.low_level.protocol.packing_instructions.GeneralAdminInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.Genquery2Input_PI;

import com.fasterxml.jackson.core.type.TypeReference;

public class IRODSResources {

	public static final class ResourceTypes {
		public static final String COMPOUND = "compound";
		public static final String DEFERRED = "deferred";
		public static final String LOAD_BALANCED = "load_balanced";
		public static final String MOCKARCHIVE = "mockarchive";
		public static final String NONBLOCKING = "nonblocking";
		public static final String PASSTHROUGH = "passthru";
		public static final String RANDOM = "random";
		public static final String REPLICATION = "replication";
		public static final String STRUCT_FILE = "structfile";
		public static final String UNIVERSAL_MSS = "univmss";
		public static final String UNIXFILESYSTEM = "unixfilesystem";
	}

	public static final class ResourceStatus {
		public static final String UP = "up";
		public static final String DOWN = "down";
	}

	public static final class ResourceInfo {
		public String id;
		public String name;
		public String type;
		public String zoneName;
		public String hostName;
		public String vaultPath;
		public String status;
		public String contextString;
		public String comments;
		public String info;
		public String freeSpace;
		public long freeSpaceTime;
		public String parentId;
		public long ctime;
		public long mtime;
		public long mtimeMillis;
	}

	public static final class ResourceRegistrationInfo {
		public String resourceName;
		public String resourceType;
		public String hostName;
		public String vaultPath;
		public String contextString;
	}

	public static class ResourceProperty {
	}

	public static final class ResourceTypeProperty extends ResourceProperty {
		public String value;
	}

	public static final class HostNameProperty extends ResourceProperty {
		public String value;
	}

	public static final class VaultPathProperty extends ResourceProperty {
		public String value;
	}

	public static final class ResourceStatusProperty extends ResourceProperty {
		public String value;
	}

	public static final class ResourceCommentsProperty extends ResourceProperty {
		public String value;
	}

	public static final class ResourceInfoProperty extends ResourceProperty {
		public String value;
	}

	public static final class FreeSpaceProperty extends ResourceProperty {
		public String value;
	}

	public static final class ContextStringProperty extends ResourceProperty {
		public String value;
	}

	public static void addResource(RcComm comm, ResourceRegistrationInfo info) throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		var location = "";
		if (null != info.hostName && !info.hostName.isEmpty()) {
			if (null != info.vaultPath && !info.vaultPath.isEmpty()) {
				location = String.format("%s:%s", info.hostName, info.vaultPath);
			}
		}

		var input = new GeneralAdminInp_PI();
		input.arg0 = "add";
		input.arg1 = "resource";
		input.arg2 = info.resourceName;
		input.arg3 = info.resourceType;
		input.arg4 = location;
		input.arg5 = (null == info.contextString) ? "" : info.contextString;
		input.arg6 = "";

		var ec = IRODSApi.rcGeneralAdmin(comm, input);
		if (ec < 0) {
			throw new IRODSException(ec, "rcGeneralAdmin error");
		}
	}

	public static void removeResource(RcComm comm, String resourceName) throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == resourceName || resourceName.isEmpty()) {
			throw new IllegalArgumentException("Resource name is null or empty");
		}

		var input = new GeneralAdminInp_PI();
		input.arg0 = "rm";
		input.arg1 = "resource";
		input.arg2 = resourceName;
		input.arg3 = ""; // Dryrun flag (cannot be null).

		var ec = IRODSApi.rcGeneralAdmin(comm, input);
		if (ec < 0) {
			throw new IRODSException(ec, "rcGeneralAdmin error");
		}
	}

	public static void addChildResource(RcComm comm, String parentResourceName, String childResourceName,
			String contextString) throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == parentResourceName || parentResourceName.isEmpty()) {
			throw new IllegalArgumentException("Parent resource name is null or empty");
		}

		if (null == childResourceName || childResourceName.isEmpty()) {
			throw new IllegalArgumentException("Child resource name is null or empty");
		}

		var input = new GeneralAdminInp_PI();
		input.arg0 = "add";
		input.arg1 = "childtoresc";
		input.arg2 = parentResourceName;
		input.arg3 = childResourceName;
		input.arg4 = (null == contextString) ? "" : contextString;

		var ec = IRODSApi.rcGeneralAdmin(comm, input);
		if (ec < 0) {
			throw new IRODSException(ec, "rcGeneralAdmin error");
		}
	}

	public static void removeChildResource(RcComm comm, String parentResourceName, String childResourceName)
			throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == parentResourceName || parentResourceName.isEmpty()) {
			throw new IllegalArgumentException("Parent resource name is null or empty");
		}

		if (null == childResourceName || childResourceName.isEmpty()) {
			throw new IllegalArgumentException("Child resource name is null or empty");
		}

		var input = new GeneralAdminInp_PI();
		input.arg0 = "rm";
		input.arg1 = "childfromresc";
		input.arg2 = parentResourceName;
		input.arg3 = childResourceName;

		var ec = IRODSApi.rcGeneralAdmin(comm, input);
		if (ec < 0) {
			throw new IRODSException(ec, "rcGeneralAdmin error");
		}
	}

	public static boolean resourceExists(RcComm comm, String resourceName) throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == resourceName || resourceName.isEmpty()) {
			throw new IllegalArgumentException("Resource name is null or empty");
		}

		var input = new Genquery2Input_PI();
		input.query_string = String.format("select RESC_ID where RESC_NAME = '%s'", resourceName);
		input.zone = comm.clientUserZone;

		var output = new Reference<String>();
		var ec = IRODSApi.rcGenQuery2(comm, input, output);
		if (ec < 0) {
			throw new IRODSException(ec, "rcGenQuery2 error");
		}

		var typeRef = new TypeReference<List<List<String>>>() {
		};
		var rows = JsonUtil.fromJsonString(output.value, typeRef);
		return !rows.isEmpty();
	}

	public static Optional<ResourceInfo> resourceInfo(RcComm comm, String resourceName)
			throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == resourceName || resourceName.isEmpty()) {
			throw new IllegalArgumentException("Resource name is null or empty");
		}

		var input = new Genquery2Input_PI();
		input.query_string = String.format("select RESC_ID, RESC_TYPE_NAME, RESC_ZONE_NAME, RESC_HOSTNAME, "
				+ "RESC_VAULT_PATH, RESC_STATUS, RESC_CONTEXT, RESC_COMMENT, RESC_INFO, "
				+ "RESC_FREE_SPACE, RESC_FREE_SPACE_TIME, RESC_PARENT, RESC_CREATE_TIME, "
				+ "RESC_MODIFY_TIME, RESC_MODIFY_TIME_MILLIS where RESC_NAME = '%s'", resourceName);
		input.zone = comm.clientUserZone;

		var output = new Reference<String>();
		var ec = IRODSApi.rcGenQuery2(comm, input, output);
		if (ec < 0) {
			throw new IRODSException(ec, "rcGenQuery2 error");
		}

		var typeRef = new TypeReference<List<List<String>>>() {
		};
		var rows = JsonUtil.fromJsonString(output.value, typeRef);
		if (rows.isEmpty()) {
			return Optional.empty();
		}

		var row = rows.get(0);
		var info = new ResourceInfo();
		info.name = resourceName;
		info.id = row.get(0);
		info.type = row.get(1);
		info.zoneName = row.get(2);
		info.hostName = row.get(3);
		info.vaultPath = row.get(4);
		info.status = row.get(5);
		info.contextString = row.get(6);
		info.comments = row.get(7);
		info.info = row.get(8);
		info.freeSpace = row.get(9);
		info.parentId = row.get(11);
		info.ctime = Long.parseLong(row.get(12));
		info.mtime = Long.parseLong(row.get(13));
		info.mtimeMillis = Long.parseLong(row.get(14));

		if (!row.get(10).isEmpty()) {
			info.freeSpaceTime = Long.parseLong(row.get(10));
		}

		return Optional.of(info);
	}

	public static void modifyResource(RcComm comm, String resourceName, ResourceProperty property)
			throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == resourceName || resourceName.isEmpty()) {
			throw new IllegalArgumentException("Resource name is null or empty");
		}

		if (null == property) {
			throw new IllegalArgumentException("Resource property is null");
		}

		var input = new GeneralAdminInp_PI();
		input.arg0 = "modify";
		input.arg1 = "resource";
		input.arg2 = resourceName;

		if (property instanceof ResourceTypeProperty p) {
			input.arg3 = "type";
			input.arg4 = p.value;
		} else if (property instanceof HostNameProperty p) {
			input.arg3 = "host";
			input.arg4 = p.value;
		} else if (property instanceof VaultPathProperty p) {
			input.arg3 = "path";
			input.arg4 = p.value;
		} else if (property instanceof ResourceStatusProperty p) {
			input.arg3 = "status";
			input.arg4 = p.value;
		} else if (property instanceof ResourceCommentsProperty p) {
			input.arg3 = "comment";
			input.arg4 = p.value;
		} else if (property instanceof ResourceInfoProperty p) {
			input.arg3 = "info";
			input.arg4 = p.value;
		} else if (property instanceof FreeSpaceProperty p) {
			input.arg3 = "free_space";
			input.arg4 = p.value;
		} else if (property instanceof ContextStringProperty p) {
			input.arg3 = "context";
			input.arg4 = p.value;
		}

		var ec = IRODSApi.rcGeneralAdmin(comm, input);
		if (ec < 0) {
			throw new IRODSException(ec, "rcGeneralAdmin error");
		}
	}

	public static void rebalanceResource(RcComm comm, String resourceName) throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == resourceName || resourceName.isEmpty()) {
			throw new IllegalArgumentException("Resource name is null or empty");
		}

		var input = new GeneralAdminInp_PI();
		input.arg0 = "modify";
		input.arg1 = "resource";
		input.arg2 = resourceName;
		input.arg3 = "rebalance";
		input.arg4 = ""; // Required to avoid segfaults in server.

		var ec = IRODSApi.rcGeneralAdmin(comm, input);
		if (ec < 0) {
			throw new IRODSException(ec, "rcGeneralAdmin error");
		}
	}

	public static Optional<String> resourceName(RcComm comm, String resourceId) throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == resourceId || resourceId.isEmpty()) {
			throw new IllegalArgumentException("Resource Id is null or empty");
		}

		var input = new Genquery2Input_PI();
		input.query_string = String.format("select RESC_NAME where RESC_ID = '%s'", resourceId);
		input.zone = comm.clientUserZone;

		var output = new Reference<String>();
		var ec = IRODSApi.rcGenQuery2(comm, input, output);
		if (ec < 0) {
			throw new IRODSException(ec, "rcGenQuery2 error");
		}

		var typeRef = new TypeReference<List<List<String>>>() {
		};
		var rows = JsonUtil.fromJsonString(output.value, typeRef);
		if (rows.isEmpty()) {
			return Optional.empty();
		}

		return Optional.of(rows.get(0).get(0));
	}

}
