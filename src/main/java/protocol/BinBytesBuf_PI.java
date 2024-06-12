package protocol;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class BinBytesBuf_PI {

	public int buflen;
	public String buf;
	
	public BinBytesBuf_PI() {}

	public BinBytesBuf_PI(String data) {
		buf = Base64.getEncoder().encodeToString(data.getBytes());
		buflen = data.length();
	}

	public String decode() {
		return new String(Base64.getDecoder().decode(buf), StandardCharsets.UTF_8);
	}

}
