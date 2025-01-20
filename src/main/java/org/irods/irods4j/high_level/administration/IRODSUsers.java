package org.irods.irods4j.high_level.administration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.irods.irods4j.api.IRODSApi;
import org.irods.irods4j.api.IRODSApi.RcComm;
import org.irods.irods4j.api.IRODSException;
import org.irods.irods4j.api.IRODSKeywords;
import org.irods.irods4j.common.JsonUtil;
import org.irods.irods4j.common.Reference;
import org.irods.irods4j.high_level.administration.IRODSZones.ZoneType;
import org.irods.irods4j.low_level.protocol.packing_instructions.GenQueryOut_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.GeneralAdminInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.Genquery2Input_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.KeyValPair_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.SpecificQueryInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.UserAdminInp_PI;

import com.fasterxml.jackson.core.type.TypeReference;

public class IRODSUsers {

	public static enum UserType {
		RODSUSER, GROUPADMIN, RODSADMIN
	}

	public static final class User {
		public String name;
		public String zone;

		public User() {
		}

		public User(String name, Optional<String> zone) {
			this.name = name;
			this.zone = zone.orElse("");
		}
	}

	public static final class Group {
		public String name;

		public Group() {
		}

		public Group(String name) {
			this.name = name;
		}
	}

	public static enum UserAuthenticationOperation {
		ADD, REMOVE
	}

	public static class UserProperty {
	}

	public static final class UserPasswordProperty extends UserProperty {
		public String value;
		public String requesterPassword;
	}

	public static final class UserTypeProperty extends UserProperty {
		public UserType value;
	}

	public static final class UserAuthenticationProperty extends UserProperty {
		public UserAuthenticationOperation op;
		public String value;
	}

	public static UserType toUserType(String v) {
		if ("rodsuser".equals(v)) {
			return UserType.RODSUSER;
		}

		if ("groupadmin".equals(v)) {
			return UserType.GROUPADMIN;
		}

		if ("rodsadmin".equals(v)) {
			return UserType.RODSADMIN;
		}

		throw new IllegalArgumentException("User type not supported");
	}

	public static String toString(UserType v) {
		switch (v) {
		case RODSUSER:
			return "rodsuser";
		case GROUPADMIN:
			return "groupadmin";
		case RODSADMIN:
			return "rodsadmin";
		}

		throw new IllegalArgumentException("User type not supported");
	}

	public static String localUniqueName(RcComm comm, User user) throws IOException, IRODSException {
		if (user.zone.isEmpty()) {
			return user.name;
		}

		var qualifiedName = new StringBuilder(user.name);

		if (!user.zone.equals(Common.getLocalZone(comm))) {
			qualifiedName.append('#');
			qualifiedName.append(user.zone);
		}

		return qualifiedName.toString();
	}

	public static void addUser(RcComm comm, User user, UserType userType, ZoneType zoneType)
			throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == user) {
			throw new IllegalArgumentException("User is null");
		}

		if (null == userType) {
			throw new IllegalArgumentException("User type is null");
		}

		if (null == zoneType) {
			throw new IllegalArgumentException("Zone type is null");
		}

		var name = localUniqueName(comm, user);

		String zone = null;
		if (ZoneType.LOCAL == zoneType) {
			zone = Common.getLocalZone(comm);
		}

		var currentUser = new User(comm.clientUsername, Optional.of(comm.clientUserZone));
		var currentUserType = type(comm, currentUser);

		// We can assume the current user's type will never be empty
		// because the user is constructed using information from the RcComm.
		if (currentUserType.get() == UserType.GROUPADMIN) {
			var input = new UserAdminInp_PI();
			input.arg0 = "mkuser";
			input.arg1 = name;
			// TODO No arg2?
			input.arg3 = zone;

			var ec = IRODSApi.rcUserAdmin(comm, input);
			if (ec < 0) {
				throw new IRODSException(ec, "rcUserAdmin error");
			}

			return;
		}

		var input = new GeneralAdminInp_PI();
		input.arg0 = "add";
		input.arg1 = "user";
		input.arg2 = name;
		input.arg3 = toString(currentUserType.get());
		input.arg4 = zone;

		var ec = IRODSApi.rcGeneralAdmin(comm, input);
		if (ec < 0) {
			throw new IRODSException(ec, "rcGeneralAdmin error");
		}
	}

	public static void removeUser(RcComm comm, User user) throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == user) {
			throw new IllegalArgumentException("User is null");
		}

		var input = new GeneralAdminInp_PI();
		input.arg0 = "rm";
		input.arg1 = "user";
		input.arg2 = localUniqueName(comm, user);
		input.arg3 = user.zone;

		var ec = IRODSApi.rcGeneralAdmin(comm, input);
		if (ec < 0) {
			throw new IRODSException(ec, "rcGeneralAdmin error");
		}
	}

	public static void modifyUser(RcComm comm, User user, UserProperty property)
			throws IOException, IRODSException, NoSuchAlgorithmException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == user) {
			throw new IllegalArgumentException("User is null");
		}

		if (null == property) {
			throw new IllegalArgumentException("User property is null");
		}

		var name = localUniqueName(comm, user);

		var input = new GeneralAdminInp_PI();
		input.arg0 = "modify";
		input.arg1 = "user";
		input.arg2 = name;

		if (property instanceof UserPasswordProperty p) {
			input.arg3 = "password";
			// TODO Don't rely on obfuscation. Allow clients to send plaintext passwords in
			// the clear. Open an issue in irods/irods for this - assuming we don't have an
			// issue already.
			input.arg4 = obfuscatePassword(p, comm.hashAlgorithm);
		} else if (property instanceof UserTypeProperty p) {
			input.arg3 = "type";
			input.arg4 = toString(p.value);
		} else if (property instanceof UserAuthenticationProperty p) {
			input.arg4 = p.value;

			if (UserAuthenticationOperation.ADD == p.op) {
				input.arg3 = "addAuth";
			} else if (UserAuthenticationOperation.REMOVE == p.op) {
				input.arg3 = "rmAuth";
			}
		}

		var ec = IRODSApi.rcGeneralAdmin(comm, input);
		if (ec < 0) {
			throw new IRODSException(ec, "rcGeneralAdmin error");
		}
	}

	public static void addGroup(RcComm comm, Group group) throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == group) {
			throw new IllegalArgumentException("Group is null");
		}

		var zone = Common.getLocalZone(comm);
		var currentUser = new User(comm.clientUsername, Optional.of(comm.clientUserZone));
		var currentUserType = type(comm, currentUser);

		// We can assume the current user's type will never be empty
		// because the user is constructed using information from the RcComm.
		if (currentUserType.get() == UserType.GROUPADMIN) {
			var input = new UserAdminInp_PI();
			input.arg0 = "mkgroup";
			input.arg1 = group.name;
			input.arg2 = "rodsgroup";
			input.arg3 = zone;

			var ec = IRODSApi.rcUserAdmin(comm, input);
			if (ec < 0) {
				throw new IRODSException(ec, "rcUserAdmin error");
			}

			return;
		}

		var input = new GeneralAdminInp_PI();
		input.arg0 = "add";
		input.arg1 = "group";
		input.arg2 = group.name;
		input.arg3 = "rodsgroup";
		input.arg4 = zone;

		var ec = IRODSApi.rcGeneralAdmin(comm, input);
		if (ec < 0) {
			throw new IRODSException(ec, "rcGeneralAdmin error");
		}
	}

	public static void removeGroup(RcComm comm, Group group) throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == group) {
			throw new IllegalArgumentException("Group is null");
		}

		var input = new GeneralAdminInp_PI();
		input.arg0 = "rm";
		input.arg1 = "group";
		input.arg2 = group.name;
		input.arg3 = Common.getLocalZone(comm);

		var ec = IRODSApi.rcGeneralAdmin(comm, input);
		if (ec < 0) {
			throw new IRODSException(ec, "rcGeneralAdmin error");
		}
	}

	public static void addUserToGroup(RcComm comm, Group group, User user) throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == group) {
			throw new IllegalArgumentException("Group is null");
		}

		if (null == user) {
			throw new IllegalArgumentException("User is null");
		}

//		var name = localUniqueName(comm, user);
		var currentUser = new User(comm.clientUsername, Optional.of(comm.clientUserZone));
		var currentUserType = type(comm, currentUser);

		// We can assume the current user's type will never be empty
		// because the user is constructed using information from the RcComm.
		if (currentUserType.get() == UserType.GROUPADMIN) {
			var input = new UserAdminInp_PI();
			input.arg0 = "modify";
			input.arg1 = "group";
			input.arg2 = group.name;
			input.arg3 = "add";
			input.arg4 = user.name;
			input.arg5 = user.zone;

			var ec = IRODSApi.rcUserAdmin(comm, input);
			if (ec < 0) {
				throw new IRODSException(ec, "rcUserAdmin error");
			}

			return;
		}

		var input = new GeneralAdminInp_PI();
		input.arg0 = "modify";
		input.arg1 = "group";
		input.arg2 = group.name;
		input.arg3 = "add";
		input.arg4 = user.name;
		input.arg5 = user.zone;

		var ec = IRODSApi.rcGeneralAdmin(comm, input);
		if (ec < 0) {
			throw new IRODSException(ec, "rcGeneralAdmin error");
		}
	}

	public static void removeUserFromGroup(RcComm comm, Group group, User user) throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == group) {
			throw new IllegalArgumentException("Group is null");
		}

		if (null == user) {
			throw new IllegalArgumentException("User is null");
		}

//		var name = localUniqueName(comm, user);
		var currentUser = new User(comm.clientUsername, Optional.of(comm.clientUserZone));
		var currentUserType = type(comm, currentUser);

		// We can assume the current user's type will never be empty
		// because the user is constructed using information from the RcComm.
		if (currentUserType.get() == UserType.GROUPADMIN) {
			var input = new UserAdminInp_PI();
			input.arg0 = "modify";
			input.arg1 = "group";
			input.arg2 = group.name;
			input.arg3 = "remove";
			input.arg4 = user.name;
			input.arg5 = user.zone;

			var ec = IRODSApi.rcUserAdmin(comm, input);
			if (ec < 0) {
				throw new IRODSException(ec, "rcUserAdmin error");
			}

			return;
		}

		var input = new GeneralAdminInp_PI();
		input.arg0 = "modify";
		input.arg1 = "group";
		input.arg2 = group.name;
		input.arg3 = "remove";
		input.arg4 = user.name;
		input.arg5 = user.zone;

		var ec = IRODSApi.rcGeneralAdmin(comm, input);
		if (ec < 0) {
			throw new IRODSException(ec, "rcGeneralAdmin error");
		}
	}

	public static List<User> users(RcComm comm) throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		var input = new Genquery2Input_PI();
		input.query_string = "select USER_NAME, USER_ZONE where USER_TYPE != 'rodsgroup'";

		var output = new Reference<String>();

		var ec = IRODSApi.rcGenQuery2(comm, input, output);
		if (ec < 0) {
			throw new IRODSException(ec, "rcGenQuery2 error");
		}

		var users = new ArrayList<User>();
		var typeRef = new TypeReference<List<List<String>>>() {
		};
		var rows = JsonUtil.fromJsonString(output.value, typeRef);
		if (!rows.isEmpty()) {
			rows.forEach(row -> users.add(new User(row.get(0), Optional.of(row.get(1)))));
		}

		return users;
	}

	public static List<User> users(RcComm comm, Group group) throws IOException, IRODSException {
		// TODO Disabled until GenQuery2 provides better support for groups.
		throw new UnsupportedOperationException("Not implemented yet");

//		if (null == comm) {
//			throw new IllegalArgumentException("RcComm is null");
//		}
//
//		if (null == group) {
//			throw new IllegalArgumentException("Group is null");
//		}
//
//		var input = new Genquery2Input_PI();
//		input.query_string = String.format(
//				"select USER_NAME, USER_ZONE where USER_TYPE != 'rodsgroup' and USER_GROUP_NAME = '%s'", group.name);
//
//		var output = new Reference<String>();
//
//		var ec = IRODSApi.rcGenQuery2(comm, input, output);
//		if (ec < 0) {
//			throw new IRODSException(ec, "rcGenQuery2 error");
//		}
//
//		var users = new ArrayList<User>();
//		var typeRef = new TypeReference<List<List<String>>>() {
//		};
//		var rows = JsonUtil.fromJsonString(output.value, typeRef);
//		if (!rows.isEmpty()) {
//			rows.forEach(row -> users.add(new User(row.get(0), Optional.of(row.get(1)))));
//		}
//
//		return users;
	}

	public static List<Group> groups(RcComm comm) throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		var input = new Genquery2Input_PI();
		input.query_string = "select USER_NAME where USER_TYPE = 'rodsgroup'";

		var output = new Reference<String>();

		var ec = IRODSApi.rcGenQuery2(comm, input, output);
		if (ec < 0) {
			throw new IRODSException(ec, "rcGenQuery2 error");
		}

		var groups = new ArrayList<Group>();
		var typeRef = new TypeReference<List<List<String>>>() {
		};
		var rows = JsonUtil.fromJsonString(output.value, typeRef);
		if (!rows.isEmpty()) {
			rows.forEach(row -> groups.add(new Group(row.get(0))));
		}

		return groups;
	}

	public static List<Group> groups(RcComm comm, User user) throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == user) {
			throw new IllegalArgumentException("User is null");
		}

		var groups = new ArrayList<Group>();

		var input = new SpecificQueryInp_PI();
		input.sql = "listGroupsForUser";
		input.arg1 = localUniqueName(comm, user);
		input.maxRows = 256; // TODO Need to think about this and pagination.
		input.KeyValPair_PI = new KeyValPair_PI();
		input.KeyValPair_PI.ssLen = 1;
		input.KeyValPair_PI.keyWord = new ArrayList<>();
		input.KeyValPair_PI.svalue = new ArrayList<>();
		input.KeyValPair_PI.keyWord.add(IRODSKeywords.ZONE);
		input.KeyValPair_PI.svalue.add(user.zone.isEmpty() ? Common.getLocalZone(comm) : user.zone);

		var output = new Reference<GenQueryOut_PI>();

		var ec = IRODSApi.rcSpecificQuery(comm, input, output);
		if (ec < 0) {
			throw new IRODSException(ec, "rcSpecificQuery error");
		}

		if (0 == output.value.rowCnt) {
			return groups;
		}

		var sqlResult = output.value.SqlResult_PI.get(1);
		sqlResult.value.forEach(group -> groups.add(new Group(group)));

		return groups;
	}

	public static boolean exists(RcComm comm, User user) throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == user) {
			throw new IllegalArgumentException("User is null");
		}

		var input = new Genquery2Input_PI();
		input.query_string = String.format(
				"select USER_ID where USER_TYPE != 'rodsgroup' and USER_NAME = '%s' and USER_ZONE = '%s'", user.name,
				(user.zone.isEmpty() ? Common.getLocalZone(comm) : user.zone));

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

	public static boolean exists(RcComm comm, Group group) throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == group) {
			throw new IllegalArgumentException("Group is null");
		}

		var input = new Genquery2Input_PI();
		input.query_string = String.format("select GROUP_ID where USER_TYPE = 'rodsgroup' and USER_NAME = '%s'",
				group.name);

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

	public static Optional<String> id(RcComm comm, User user) throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == user) {
			throw new IllegalArgumentException("User is null");
		}

		var input = new Genquery2Input_PI();
		input.query_string = String.format(
				"select USER_ID where USER_TYPE != 'rodsgroup' and USER_NAME = '%s' and USER_ZONE = '%s'", user.name,
				(user.zone.isEmpty() ? Common.getLocalZone(comm) : user.zone));

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

	public static Optional<String> id(RcComm comm, Group group) throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == group) {
			throw new IllegalArgumentException("Group is null");
		}

		var input = new Genquery2Input_PI();
		input.query_string = String.format("select GROUP_ID where USER_TYPE = 'rodsgroup' and USER_NAME = '%s'",
				group.name);

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

	public static Optional<UserType> type(RcComm comm, User user) throws IOException, IRODSException {
		if (null == comm) {
			throw new IllegalArgumentException("RcComm is null");
		}

		if (null == user) {
			throw new IllegalArgumentException("User is null");
		}

		var input = new Genquery2Input_PI();
		input.query_string = String.format(
				"select USER_TYPE where USER_TYPE != 'rodsgroup' and USER_NAME = '%s' and USER_ZONE = '%s'", user.name,
				(user.zone.isEmpty() ? Common.getLocalZone(comm) : user.zone));

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

		return Optional.of(toUserType(rows.get(0).get(0)));
	}

	public static boolean userIsMemberOfGroup(RcComm comm, Group group, User user) {
		// TODO Disabled until GenQuery2 provides better support for groups.
		throw new UnsupportedOperationException("Not implemented yet");
	}

	private static String obfuscatePassword(UserPasswordProperty p, String hashAlgo) throws NoSuchAlgorithmException {
		final var MAX_PASSWORD_LEN = 50;

		var plainTextPasswordSb = new StringBuilder();
		plainTextPasswordSb.append(p.value);
		plainTextPasswordSb.setLength(MAX_PASSWORD_LEN + 10);

		var count = MAX_PASSWORD_LEN - 10 - p.value.length();
		if (count > 15) {
			// The random sequence of characters is used for padding and must match
			// what is defined on the server-side.
			plainTextPasswordSb.append("1gCBizHWbwIYyWLoysGzTe6SyzqFKMniZX05faZHWAwQKXf6Fs".substring(0, count));
		}

		var keySb = new StringBuilder();
		if (p.requesterPassword.length() >= MAX_PASSWORD_LEN) {
			throw new IllegalArgumentException("Requester password exceeds max key size: " + MAX_PASSWORD_LEN);
		}
		keySb.append(p.requesterPassword.substring(0, Math.min(p.requesterPassword.length(), MAX_PASSWORD_LEN)));

		return obfEncodeByKey(plainTextPasswordSb.toString(), keySb.toString(), hashAlgo);
	}

	// This function is a port of obfEncodeByKey() in
	// irods/irods/lib/core/src/obf.cpp.
	private static String obfEncodeByKey(String data, String key, String hashAlgo) throws NoSuchAlgorithmException {
//		var keyBuf = new int[100 + 1]; // +1 for null terminating byte.
//		var keyBytes = key.getBytes(StandardCharsets.UTF_8);
//		var length = Math.min(keyBuf.length - 1, key.length());
//		for (int x = 0; x < length; ++x) {
//			keyBuf[x] = (int) (keyBytes[x] & 0xff);
//		}
		var keyBuf = new int[100];
		var keyBytes = key.getBytes(StandardCharsets.UTF_8);
		var length = Math.min(keyBuf.length, key.length());
		for (int x = 0; x < length; ++x) {
			keyBuf[x] = keyBytes[x] & 0xff;
		}

		// TODO Must also support SHA1.
		// Get the MD5 digest of the key to get some bytes with many different values.
//		var hexKey = obfMakeOneWayHash("md5", keyBuf, keyBuf.length - 1);
//		var buffer = new int[64 + 1]; // Each digest is 16 bytes, 4 of them.
		var hexKey = obfMakeOneWayHash(hashAlgo, keyBuf, keyBuf.length);
		var buffer = new int[64]; // Each digest is 16 bytes, 4 of them.
		copyIntArrayToByteArray(buffer, 0, hexKey, hexKey.length);

		// Hash of the hash.
		var v = obfMakeOneWayHash(hashAlgo, buffer, 16);
		copyIntArrayToByteArray(buffer, 16, v, v.length);

		// Hash of two hashes.
		v = obfMakeOneWayHash(hashAlgo, buffer, 32);
		copyIntArrayToByteArray(buffer, 32, v, v.length);

		// Hash of two hashes.
		v = obfMakeOneWayHash(hashAlgo, buffer, 32);
		copyIntArrayToByteArray(buffer, 48, v, v.length);

		final var MAX_PASSWORD_LEN = 50;

		var inSb = new StringBuilder(data);
		inSb.setLength(MAX_PASSWORD_LEN + 10);

		var outSb = new StringBuilder();
		outSb.setLength(MAX_PASSWORD_LEN + 100);

		// TODO
//		if (HASH_ALGO_SHA1 == defaultHashAlgo) {
//			inSb.setCharAt(cpOut++, 's');
//			inSb.setCharAt(cpOut++, 'h');
//			inSb.setCharAt(cpOut++, 'a');
//			inSb.setCharAt(cpOut++, '1');
//		}

		var wheel = new int[26 + 26 + 10 + 15];
		int j = 0;
		for (var i = 0; i < 10; ++i) {
			wheel[j++] = (int) '0' + i;
		}
		for (var i = 0; i < 26; ++i) {
			wheel[j++] = (int) 'A' + i;
		}
		for (var i = 0; i < 26; ++i) {
			wheel[j++] = (int) 'a' + i;
		}
		for (var i = 0; i < 15; ++i) {
			wheel[j++] = (int) '!' + i;
		}

		var cpKey = 0;
		var pc = 0; // Previous character.
		for (int cpIn = 0, cpOut = 0;; ++cpIn) {
			var k = buffer[cpKey++];
			if (cpKey > 60) {
				cpKey = 0;
			}

			var found = false;
			for (var i = 0; i < wheel.length; ++i) {
				if ((int) inSb.charAt(cpIn) == wheel[i]) {
					j = i + k + pc;
					j %= wheel.length;
					outSb.setCharAt(cpOut++, (char) (wheel[j] & 0xff));

					// "cipherBlockChaining" is not supported by this implementation. Based on the C
					// implementation, it is only enabled through use of functions which are not
					// supported by this implementation, therefore it is irrelevant.
//					if (cipherBlockChaining) {
//						pc = cpOut - 1;
//					}

					found = true;
					break;
				}
			}

			if (!found) {
				if (inSb.charAt(cpIn) == 0) {
					outSb.setLength(cpOut);
					return outSb.toString();
				} else {
					outSb.setCharAt(cpOut++, inSb.charAt(cpIn));
				}
			}
		}
	}

	private static byte[] obfMakeOneWayHash(String hashAlgo, int[] buffer, int bufferLength)
			throws NoSuchAlgorithmException {
		// Convert int array to a unsigned byte array.
		var bufUnsigned = new byte[bufferLength];
		for (int x = 0; x < bufferLength; ++x) {
			bufUnsigned[x] = (byte) (buffer[x] & 0xff);
		}

		// Hash the buffer.
		var hasher = MessageDigest.getInstance(hashAlgo);
		return hasher.digest(bufUnsigned);
	}

	private static void copyIntArrayToByteArray(int[] dst, int dstOffset, byte[] src, int count) {
		for (int i = 0; i < src.length; ++i) {
			dst[i + dstOffset] = src[i] & 0xff;
		}
	}

}
