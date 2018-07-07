package mmdb.kvs.app.output;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FineGrainedPairs {
	
	List<FineGrainedPair> FineGrainedPairs;

	public FineGrainedPairs (List<FineGrainedPair> FineGrainedPairs) {
		this.FineGrainedPairs=FineGrainedPairs;
	}
	
	@JsonProperty
	public List<FineGrainedPair> getFineGrainedPairs() {
		return FineGrainedPairs;
	}

	public void setFineGrainedPairs(List<FineGrainedPair> FineGrainedPairs) {
		this.FineGrainedPairs = FineGrainedPairs;
	}

}
