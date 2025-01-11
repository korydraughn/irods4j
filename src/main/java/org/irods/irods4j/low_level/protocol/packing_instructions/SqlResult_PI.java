package org.irods.irods4j.low_level.protocol.packing_instructions;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

public class SqlResult_PI {

	public int attriInx;
	public int reslen;
	public List<String> value;

	public static void main(String[] args) throws JsonProcessingException {
		XmlMapper m = new XmlMapper();
		var pi = new SqlResult_PI();
		pi.attriInx = 3;
		pi.reslen = 4;
		pi.value = new ArrayList<>();
        pi.value.add("Kory");
        pi.value.add("Draughn");
        pi.value.add("36");
		System.out.println(m.writeValueAsString(pi));
	}

}
