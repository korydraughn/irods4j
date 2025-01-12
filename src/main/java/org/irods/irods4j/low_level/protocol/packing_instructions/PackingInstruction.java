package org.irods.irods4j.low_level.protocol.packing_instructions;

public interface PackingInstruction {
	
	public String pack();
	
	public <T> T unpack();

}
