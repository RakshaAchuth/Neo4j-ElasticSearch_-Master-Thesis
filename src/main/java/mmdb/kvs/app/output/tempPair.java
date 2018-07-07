
package mmdb.kvs.app.output;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class tempPair {
	private String key;
	
	@JsonProperty
	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}
	
}
