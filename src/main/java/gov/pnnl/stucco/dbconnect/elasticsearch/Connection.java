package gov.ornl.stucco.elasticsearch;

import java.util.List;
import java.util.Map;

import gov.ornl.stucco.graph_extractors.SituGraphExtractor;

import java.net.UnknownHostException; 

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchHit;

import org.json.JSONObject;
import org.json.JSONArray; 

public class Connection {
	private PreBuiltTransportClient prebuiltClient = null;
	private InetSocketTransportAddress inetAddress = null;
	private TransportClient client = null;
	
	public Connection(PreBuiltTransportClient prebuiltClient, InetSocketTransportAddress inetAddress) {
		this.prebuiltClient = prebuiltClient;
		this.inetAddress = inetAddress;
	}

	public void open() throws UnknownHostException {
  		client = prebuiltClient.addTransportAddress(inetAddress);
	}

	public void close() {
		client.close();
	}

	public void printDBContent() {
		StringBuilder dbContent = new StringBuilder();
		SearchResponse response = client.prepareSearch().get();
		SearchHit[] results = response.getHits().getHits();
		for(SearchHit hit : results){
				System.out.println(hit);
		    String entry = hit.getSourceAsString();
		    dbContent.append(entry);
		    dbContent.append("\n");
		}
		System.out.println(dbContent.toString());

		JSONObject graph = extractGraph(dbContent.toString());
		System.out.println(graph.toString(2));
	}

	private JSONObject extractGraph(String situInfo) {
		SituGraphExtractor situExtractor = new SituGraphExtractor(situInfo);
		JSONObject graph = situExtractor.getGraph();

		return graph;
	}

	List<Map<String, Object>> getInEdgesPage(String vertName, int pageNumber, int pageSize) {
		return null;
	}

	List<Map<String, Object>> getOutEdgesPage(String vertName, int pageNumber, int pageSize) {
		return null;
	}

	Map<String,Object> getVertByName(String vertName) { //vertName is port or ip address, etc. 
		return null;
	}

	List<Map<String, Object>> getVertsByConstraints(Map<String, List<Object>> constraints, int pageNumber, int pageSize) {
		return null;
	}


}