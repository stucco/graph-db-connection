package gov.pnnl.stucco.dbconnect.elasticsearch;

import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

import gov.ornl.stucco.graph_extractors.SituGraphExtractor;
import gov.ornl.stucco.utils.GraphUtils;
import gov.pnnl.stucco.dbconnect.DBConstraint; 

public class Connection {
	private TransportClient prebuiltClient = null;
	private InetSocketTransportAddress inetAddress = null;
	private TransportClient client = null;
	private SearchRequestBuilder searchRequest = null;
	private Logger logger;
	
	/* presetting source set to use as a source field in every vertex */		
	private Set<String> source = new HashSet<String>();
	
	public Connection(TransportClient prebuiltClient, InetSocketTransportAddress inetAddress, Logger logger) {
		this.prebuiltClient = prebuiltClient;
		this.inetAddress = inetAddress;
		this.logger = logger;
		source.add("Situ");
	}

	
	public void open() throws UnknownHostException {
  		client = prebuiltClient.addTransportAddress(inetAddress);
  		searchRequest = client.prepareSearch("situ_2015-07-10");
	}

	
	public void close() {
		client.close();
	}

	
//	public void printDBContent() {
//		StringBuilder dbContent = new StringBuilder();
//		SearchResponse response = client.prepareSearch("situ_2015-07-10").get();
//		SearchHit[] results = response.getHits().getHits();
//		for(SearchHit hit : results){
//			System.out.println(hit);
//		    String entry = hit.getSourceAsString();
//		    dbContent.append(entry);
//		    dbContent.append("\n");
//		}
//		System.out.println(dbContent.toString());
//
//		JSONObject graph = extractGraph(dbContent.toString());
//		System.out.println(graph.toString(2));
//	}
//
//	
//	private JSONObject extractGraph(String situInfo) {
//		SituGraphExtractor situExtractor = new SituGraphExtractor(situInfo);
//		JSONObject graph = situExtractor.getGraph();
//
//		return graph;
//	}
	
	//TODO: Use index, source name, etc from config yml file
	
	public JSONArray getInEdgesPage(String vertName, String vertType, int pageNumber, int pageSize) {
		JSONArray edgeArray = new JSONArray();
		JSONObject vertJSON = new JSONObject();
		
		if (client != null) {
			if (vertType.equalsIgnoreCase("flow")) {
				
				Pattern p = Pattern.compile("^(\\d{1,3}\\.){3}\\d{1,3}:\\d+_through_(\\d{1,3}\\.){3}\\d{1,3}:\\d+$");
				Matcher m = p.matcher(vertName);
				if (m.matches()) {
					String[] flowPieces = vertName.split("_through_");
					String[] srcAddr = flowPieces[0].split(":");
					String[] dstAddr = flowPieces[1].split(":");
					if ((srcAddr.length == 2) && (dstAddr.length == 2)) {
						String srcIP = srcAddr[0];
						String srcPort = srcAddr[1];
						String dstIP = dstAddr[0];
						String dstPort = dstAddr[1];
						SearchResponse response = searchRequest.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("srcIP", srcIP)).must(QueryBuilders.termQuery("srcPort", srcPort)).must(QueryBuilders.termQuery("dstIP", dstIP)).must(QueryBuilders.termQuery("dstPort", dstPort))).setSize(1).get();
						if (response.getHits().getTotalHits() >= 1) {
							String srcAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setAddressJson(srcAddressID, srcIP, null, srcPort, null, source, "Situ");
							edgeArray.put(vertJSON);
							String dstAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setAddressJson(dstAddressID, dstIP, null, dstPort, null, source, "Situ");
							edgeArray.put(vertJSON);
						}
					}
				}
				
			} 
			else if (vertType.equalsIgnoreCase("address")) {
				
				Pattern p = Pattern.compile("^(\\d{1,3}\\.){3}\\d{1,3}:\\d+$");
				Matcher m = p.matcher(vertName);
				if (m.matches()) {
					String[] addressPieces = vertName.split(":");
					if (addressPieces.length == 2) {
						String ip = addressPieces[0];
						String port = addressPieces[1];
						SearchResponse response = searchRequest.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("srcIP", ip)).must(QueryBuilders.termQuery("srcPort", port))).setSize(1).get();
						if (response.getHits().getTotalHits() >= 1) {
							String ipID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setIpJson(ipID, ip, source, "Situ");
							edgeArray.put(vertJSON);
							String portID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setPortJson(portID, port, source, "Situ");
							edgeArray.put(vertJSON);
						}
						else {
							response = searchRequest.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("dstIP", ip)).must(QueryBuilders.termQuery("dstPort", port))).setSize(1).get();
							if (response.getHits().getTotalHits() >= 1) {
								String ipID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
								vertJSON = GraphUtils.setIpJson(ipID, ip, source, "Situ");
								edgeArray.put(vertJSON);
								String portID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
								vertJSON = GraphUtils.setPortJson(portID, port, source, "Situ");
								edgeArray.put(vertJSON);
							}
						}
					}
				}
				
			} 
			else {
				logger.warn("Unknown vertex type '" + vertType + "' encountered in getInEdgesPage().");
			}
		}
		System.out.println(edgeArray);
		return edgeArray;
	}

	
	public JSONArray getOutEdgesPage(String vertName, String vertType, int pageNumber, int pageSize) {
		JSONArray edgeArray = new JSONArray();
		JSONObject vertJSON = new JSONObject();
		
		if (client != null) {
			if (vertType.equalsIgnoreCase("ip")) {
				
				
				
			} 
			else if (vertType.equalsIgnoreCase("port")) {
				
				
				
			} 
			else if (vertType.equalsIgnoreCase("address")) {
				
				Pattern p = Pattern.compile("^(\\d{1,3}\\.){3}\\d{1,3}:\\d+$");
				Matcher m = p.matcher(vertName);
				if (m.matches()) {
					String[] addressPieces = vertName.split(":");
					if (addressPieces.length == 2) {
						String ip = addressPieces[0];
						String port = addressPieces[1];
						
					}
				}
				
			} 
			else {
				logger.warn("Unknown vertex type '" + vertType + "' encountered in getOutEdgesPage().");
			}
		}
		
		System.out.println(edgeArray);
		return edgeArray;
	}

	
	public JSONObject getVertByName(String vertName) { //vertName is port or ip address, etc.
		JSONObject vertJSON = null;
				
		if (client != null) {
			Pattern p = Pattern.compile("^\\d+$");
			Matcher m = p.matcher(vertName);
			if (m.matches()) {				
				SearchResponse response = searchRequest.setQuery(QueryBuilders.multiMatchQuery((String)vertName, "srcPort", "dstPort")).setSize(1).get();
				long totalHits = response.getHits().getTotalHits();
				if (totalHits >= 1) {
//					SearchHit result = response.getHits().getHits()[0];
					String portID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
					vertJSON = GraphUtils.setPortJson(portID, vertName, source, "Situ");
				}
				return vertJSON;
			}
			
			p = Pattern.compile("^(\\d{1,3}\\.){3}\\d{1,3}$");
			m = p.matcher(vertName);
			if (m.matches()) {
				SearchResponse response = searchRequest.setQuery(QueryBuilders.multiMatchQuery((String)vertName, "srcIP", "dstIP")).setSize(1).get();
				long totalHits = response.getHits().getTotalHits();
				if (totalHits >= 1) {
//					SearchHit result = response.getHits().getHits()[0];
					String ipID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
					vertJSON = GraphUtils.setIpJson(ipID, vertName, source, "Situ");
				}
				return vertJSON;
			}
			
			p = Pattern.compile("^(\\d{1,3}\\.){3}\\d{1,3}:\\d+$");
			m = p.matcher(vertName);
			if (m.matches()) {
				String[] addressPieces = vertName.split(":");
				if (addressPieces.length == 2) {
					String ip = addressPieces[0];
					String port = addressPieces[1];
					SearchResponse response = searchRequest.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("srcIP", ip)).must(QueryBuilders.termQuery("srcPort", port))).setSize(1).get();
					long totalHits = response.getHits().getTotalHits();
					if (totalHits >= 1) {
						String srcAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
						vertJSON = GraphUtils.setAddressJson(srcAddressID, ip, null, port, null, source, "Situ");
					}
					else {
						response = client.prepareSearch("situ_2015-07-10").setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("dstIP", ip)).must(QueryBuilders.termQuery("dstPort", port))).setSize(1).get();
						totalHits = response.getHits().getTotalHits();
						if (totalHits >= 1) {
							String dstAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setAddressJson(dstAddressID, ip, null, port, null, source, "Situ");
						}
					}
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
					SearchResponse response = searchRequest.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("srcIP", srcIP)).must(QueryBuilders.termQuery("srcPort", srcPort)).must(QueryBuilders.termQuery("dstIP", dstIP)).must(QueryBuilders.termQuery("dstPort", dstPort))).setSize(1).get();
					long totalHits = response.getHits().getTotalHits();
					if (totalHits >= 1) {
						SearchHit result = response.getHits().getHits()[0];
						Map<String, Object> sourceMap = result.getSource();
						String flowID = GraphUtils.buildString("stucco:Observable-", result.getId());
						String srcAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
						String dstAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
						JSONObject flowSource = new JSONObject(result.getSourceAsString());
						vertJSON = GraphUtils.setFlowJson(flowID, srcIP, srcPort, srcAddressID, dstIP, dstPort, dstAddressID, sourceMap.get("proto").toString(), source, "Situ", flowSource);
					}
					return vertJSON;
				}
			}
			
		}
		
		return vertJSON;
	}
	
	
	public JSONArray getVertByType(String vertType, int pageNumber, int pageSize) {
		JSONArray vertArray = new JSONArray();
		JSONObject vertJSON = null;
		
		if (client != null) {
			if (vertType.equalsIgnoreCase("ip")) {
				Set<String> ipSet = new HashSet<String>();
				SearchResponse response = searchRequest.addAggregation(AggregationBuilders.terms("srcIPs").field("srcIP").size(pageSize*3)).addAggregation(AggregationBuilders.terms("dstIPs").field("dstIP").size(pageSize*3)).execute().actionGet();
				Terms ips = response.getAggregations().get("srcIPs");
								
				for (Terms.Bucket entry : ips.getBuckets()) {
					ipSet.add(entry.getKeyAsString());
				}
				ips = response.getAggregations().get("dstIPs");
				for (Terms.Bucket entry : ips.getBuckets()) {
					ipSet.add(entry.getKeyAsString());
				}
				
				String[] ipArray = new String[ipSet.size()];
				ipArray = ipSet.toArray(ipArray);
				for (int i=(pageNumber*pageSize); i<Math.min((pageNumber+1)*pageSize,ipArray.length); i++) {
					String ipID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
					vertJSON = GraphUtils.setIpJson(ipID, ipArray[i], source, "Situ");
					vertArray.put(vertJSON);
				}
				
			} 
			else if (vertType.equalsIgnoreCase("port")) {
				Set<String> portSet = new HashSet<String>();
				SearchResponse response = searchRequest.addAggregation(AggregationBuilders.terms("srcPorts").field("srcPort").size(pageSize*3)).addAggregation(AggregationBuilders.terms("dstPorts").field("dstPort").size(pageSize*3)).execute().actionGet();
				Terms ports = response.getAggregations().get("srcPorts");
								
				for (Terms.Bucket entry : ports.getBuckets()) {
					portSet.add(entry.getKeyAsString());
				}
				ports = response.getAggregations().get("dstPorts");
				for (Terms.Bucket entry : ports.getBuckets()) {
					portSet.add(entry.getKeyAsString());
				}
				
				String[] portArray = new String[portSet.size()];
				portArray = portSet.toArray(portArray);
				for (int i=(pageNumber*pageSize); i<Math.min((pageNumber+1)*pageSize,portArray.length); i++) {
					String portID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
					vertJSON = GraphUtils.setPortJson(portID, portArray[i], source, "Situ");
					vertArray.put(vertJSON);
				}
				
			} 
			else if (vertType.equalsIgnoreCase("address")) {
				Set<String> addrSet = new HashSet<String>();
				SearchResponse response = searchRequest.setSize(pageSize*4).get();
				SearchHit[] hits = response.getHits().getHits();
				for (SearchHit hit : hits) {
					StringBuilder sb = new StringBuilder();
					sb.append(hit.getSource().get("srcIP").toString());
					sb.append(":");
					sb.append(hit.getSource().get("srcPort").toString());
					addrSet.add(sb.toString());
					
					sb = new StringBuilder();
					sb.append(hit.getSource().get("dstIP").toString());
					sb.append(":");
					sb.append(hit.getSource().get("dstPort").toString());
					addrSet.add(sb.toString());
				}
				
				String[] addrArray = new String[addrSet.size()];
				addrArray = addrSet.toArray(addrArray);
				for (int i=(pageNumber*pageSize); i<Math.min((pageNumber+1)*pageSize,addrArray.length); i++) {
					String[] addr = addrArray[i].split(":");
					String addrID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
					vertJSON = GraphUtils.setAddressJson(addrID, addr[0], null, addr[1], null, source, "Situ");
					vertArray.put(vertJSON);
				}
				
			} 
			else if (vertType.equalsIgnoreCase("flow")) {
				SearchResponse response = searchRequest.setSize(pageSize).get();
				SearchHit[] hits = response.getHits().getHits();
				
				for (SearchHit hit : hits) {
					Map<String, Object> sourceMap = hit.getSource();
					String flowID = GraphUtils.buildString("stucco:Observable-", hit.getId());
					String srcAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
					String dstAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
					JSONObject flowSource = new JSONObject(hit.getSourceAsString());
					vertJSON = GraphUtils.setFlowJson(flowID, (sourceMap.get("srcIP").toString()), (sourceMap.get("srcPort").toString()), srcAddressID, (sourceMap.get("dstIP").toString()), (sourceMap.get("dstPort").toString()), dstAddressID, (sourceMap.get("proto").toString()), source, "Situ", flowSource);
					vertArray.put(vertJSON);
				}
								
			} else {
				logger.warn("Unknown vertex type '" + vertType + "' encountered in getVertByType().");
			}
		}
		System.out.println(vertArray);
		return vertArray;
	}

	
	public JSONArray getVertsByConstraints(List<DBConstraint> constraints, int pageNumber, int pageSize) {
		return null;
	}

	
	long countVertsByConstraints(List<DBConstraint> constraints) {
		return -1;
	}
}