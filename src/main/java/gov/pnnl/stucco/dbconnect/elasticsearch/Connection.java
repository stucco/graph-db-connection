package gov.pnnl.stucco.dbconnect.elasticsearch;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.json.JSONArray;
import org.json.JSONObject;

import gov.ornl.stucco.graph_extractors.SituGraphExtractor;
import gov.ornl.stucco.utils.GraphUtils;
import gov.pnnl.stucco.dbconnect.DBConstraint; 

public class Connection {
	private TransportClient prebuiltClient = null;
	private InetSocketTransportAddress inetAddress = null;
	private TransportClient client = null;
	
	public Connection(TransportClient prebuiltClient, InetSocketTransportAddress inetAddress) {
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
		SearchResponse response = client.prepareSearch("situ_2015-07-10").get();
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
	

	List<Map<String, Object>> getInEdgesPage(String vertName, String vertType, int pageNumber, int pageSize) {
		return null;
	}

	
	List<Map<String, Object>> getOutEdgesPage(String vertName, String vertType, int pageNumber, int pageSize) {
		return null;
	}

	
	JSONObject getVertByName(String vertName) { //vertName is port or ip address, etc.
		JSONObject vertJSON = null;
		/* presetting source set to use as a source field in every vertex */		
		Set<String> source = new HashSet<String>();
		source.add("Situ");
		
		if (client != null) {
			Pattern p = Pattern.compile("^\\d+$");
			Matcher m = p.matcher(vertName);
			if (m.matches()) {				
				SearchResponse response = client.prepareSearch("situ_2015-07-10").setQuery(QueryBuilders.multiMatchQuery((String)vertName, "srcPort", "dstPort")).get();
				long totalHits = response.getHits().getTotalHits();
				System.out.println("Total Port Hits: " + totalHits);
				if (totalHits >= 1) {
					SearchHit result = response.getHits().getHits()[0];
					if ((result.getSource().containsKey("srcPort")) && (result.getSource().get("srcPort").toString().equals(vertName))) {
						String srcPortID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
						vertJSON = GraphUtils.setPortJson(srcPortID, vertName, source, "Situ");
					}
					else if ((result.getSource().containsKey("dstPort")) && (result.getSource().get("dstPort").toString().equals(vertName))) {
						String dstPortID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
						vertJSON = GraphUtils.setPortJson(dstPortID, vertName, source, "Situ");
					}
				}
				System.out.println(vertJSON.toString());
				return vertJSON;
			}
			
			p = Pattern.compile("^(\\d{1,3}\\.){3}\\d{1,3}$");
			m = p.matcher(vertName);
			if (m.matches()) {
				SearchResponse response = client.prepareSearch("situ_2015-07-10").setQuery(QueryBuilders.multiMatchQuery((String)vertName, "srcIP", "dstIP")).get();
				long totalHits = response.getHits().getTotalHits();
				System.out.println("Total IP Hits: " + totalHits);
				if (totalHits >= 1) {
					SearchHit result = response.getHits().getHits()[0];
					if ((result.getSource().containsKey("srcIP")) && (result.getSource().get("srcIP").equals(vertName))) {
						String srcIpID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
						vertJSON = GraphUtils.setIpJson(srcIpID, vertName, source, "Situ");
					}
					else if ((result.getSource().containsKey("dstIP")) && (result.getSource().get("dstIP").equals(vertName))) {
						String dstIpID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
						vertJSON = GraphUtils.setIpJson(dstIpID, vertName, source, "Situ");
					}
				}
				System.out.println(vertJSON.toString());
				return vertJSON;
			}
			
			p = Pattern.compile("^(\\d{1,3}\\.){3}\\d{1,3}:\\d+$");
			m = p.matcher(vertName);
			if (m.matches()) {
				String[] addressPieces = vertName.split(":");
				if (addressPieces.length == 2) {
					String ip = addressPieces[0];
					String port = addressPieces[1];
					SearchResponse response = client.prepareSearch("situ_2015-07-10").setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("srcIP", ip)).must(QueryBuilders.termQuery("srcPort", port))).get();
					long totalHits = response.getHits().getTotalHits();
					System.out.println("Total Src Address Hits: " + totalHits);
					if (totalHits >= 1) {
						String srcAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
						vertJSON = GraphUtils.setAddressJson(srcAddressID, ip, null, port, null, source, "Situ");
					}
					else {
						response = client.prepareSearch("situ_2015-07-10").setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("dstIP", ip)).must(QueryBuilders.termQuery("dstPort", port))).get();
						totalHits = response.getHits().getTotalHits();
						System.out.println("Total Dst Address Hits: " + totalHits);
						if (totalHits >= 1) {
							String dstAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setAddressJson(dstAddressID, ip, null, port, null, source, "Situ");
						}
					}
					System.out.println(vertJSON.toString());
					return vertJSON;
				}
			}
			
			p = Pattern.compile("^(\\d{1,3}\\.){3}\\d{1,3}:\\d+_through_(\\d{1,3}\\.){3}\\d{1,3}:\\d+$");
			m = p.matcher(vertName);
			if (m.matches()) {
				String[] flowPieces = vertName.split("_through_");
				String[] srcAddr = flowPieces[0].split(":");
				String[] dstAddr = flowPieces[1].split(":");
				if ((srcAddr.length == 2) && (dstAddr.length == 2)) {
					String srcIP = srcAddr[0];
					String srcPort = srcAddr[1];
					String dstIP = dstAddr[0];
					String dstPort = dstAddr[1];
					SearchResponse response = client.prepareSearch("situ_2015-07-10").setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("srcIP", srcIP)).must(QueryBuilders.termQuery("srcPort", srcPort)).must(QueryBuilders.termQuery("dstIP", dstIP)).must(QueryBuilders.termQuery("dstPort", dstPort))).get();
					long totalHits = response.getHits().getTotalHits();
					System.out.println("Total Flow Hits: " + totalHits);
					if (totalHits >= 1) {
						SearchHit result = response.getHits().getHits()[0];
						Map<String, Object> sourceMap = result.getSource();
						String flowID = GraphUtils.buildString("stucco:Observable-", result.getId());
						String srcAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
						String dstAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
						JSONObject flowSource = new JSONObject(result.getSourceAsString());
						vertJSON = GraphUtils.setFlowJson(flowID, srcIP, srcPort, srcAddressID, dstIP, dstPort, dstAddressID, sourceMap.get("proto").toString(), source, "Situ", flowSource);
					}
					System.out.println(vertJSON.toString());
					return vertJSON;
				}
			}
			
		}
		return null;
	}
	
	
	Map<String,Object> getVertByType(String vertType) {
		Map<String,Object> mapResults = new HashMap<String,Object>();
		
		return mapResults;
	}

	
	JSONArray getVertsByConstraints(Map<String, List<Object>> constraints, int pageNumber, int pageSize) {
		return null;
	}

	
	long countVertsByConstraints(List<DBConstraint> constraints) {
		return -1;
	}
}