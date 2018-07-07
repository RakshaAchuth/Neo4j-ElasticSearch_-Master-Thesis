
package mmdb.kvs.app.output;

import java.util.ArrayList;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class pair {
	private String key;
	
	private Map<String,String> value;
	
	@JsonProperty
	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}
	
	@JsonProperty
	public Map<String,String> getValue() {
		
		return value;
	}

	public void setValue(Map<String, String> value) {
		this.value = value;
	}
}
