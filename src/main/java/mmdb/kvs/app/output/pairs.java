package mmdb.kvs.app.output;

import java.util.List;
import mmdb.kvs.app.output.pair;

import com.fasterxml.jackson.annotation.JsonProperty;

public class pairs {
	
	List<pair> pairs;

	public pairs (List<pair> pairs) {
		this.pairs=pairs;
	}
	
	@JsonProperty
	public List<pair> getPairs() {
		return pairs;
	}

	public void setPairs(List<pair> pairs) {
		this.pairs = pairs;
	}

}