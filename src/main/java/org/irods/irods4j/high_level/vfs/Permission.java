package org.irods.irods4j.high_level.vfs;

public enum Permission {

    NULL,
    READ_METADATA,
    READ_OBJECT,
//	READ,
    CREATE_METADATA,
    MODIFY_METADATA,
    DELETE_METADATA,
    CREATE_OBJECT,
    MODIFY_OBJECT,
//	WRITE,
    DELETE_OBJECT,
    OWN

}
