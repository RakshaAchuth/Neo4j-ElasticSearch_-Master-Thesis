
public class Relationships {
	List<Relationship> relationships;
	
	
}
public Relationships(List<Relationship> relationship) {
	this.relationship = relationship;
}

@JsonProperty
public List<Relationship> getRelationship() {
	return Relationships;
}

public void setRelationship(List<Relationship>) {
	this.relationships = relationship;