
public class Vertexes {
	List<Vertex> vertex;
}

public Vertexes(List<Vertex> vertex) {
	this.vertex = vertex;
}

@JsonProperty
public List<Vertex> getVertex() {
	return Vertex;
}

public void setVertex(List<Vertex>) {
	this.Vertexes = vertex;
}
