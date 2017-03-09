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
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

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
						SearchResponse response = searchRequest.setQuery(QueryBuilders.boolQuery()
								.should(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("srcIP", ip)).must(QueryBuilders.termQuery("srcPort", port)))
								.should(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("dstIP", ip)).must(QueryBuilders.termQuery("dstPort", port))))
								.setSize(1).get();
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
			else {
				logger.warn("Unknown vertex type '" + vertType + "' encountered in getInEdgesPage().");
			}
		}
		
		return edgeArray;
	}

	
	public JSONArray getOutEdgesPage(String vertName, String vertType, int pageNumber, int pageSize) {
		JSONArray edgeArray = new JSONArray();
		JSONObject vertJSON = new JSONObject();
		
		int startIndex = pageNumber * pageSize;
		
		if (client != null) {
			if (vertType.equalsIgnoreCase("ip")) {
				
				Pattern p = Pattern.compile("^(\\d{1,3}\\.){3}\\d{1,3}$");
				Matcher m = p.matcher(vertName);
				if (m.matches()) {
					Set<String> addrSet = new HashSet<String>();
					SearchResponse response = searchRequest.setQuery(QueryBuilders.boolQuery()
							.should(QueryBuilders.termQuery("srcIP", vertName))
							.should(QueryBuilders.termQuery("dstIP", vertName))).get();
					for (SearchHit hit : response.getHits().getHits()) {
						Map<String,Object> sourceMap = hit.getSource();
						if (sourceMap.get("srcIP").toString().equalsIgnoreCase(vertName)) {
							
							StringBuilder addrName = new StringBuilder();
							addrName.append(vertName);
							addrName.append(":");
							addrName.append(sourceMap.get("srcPort").toString());
							
							if (!addrSet.contains(addrName.toString())) {
								addrSet.add(addrName.toString());
								if (addrSet.size() >= (pageNumber * pageSize + 1)) {
									String addrID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
									vertJSON = GraphUtils.setAddressJson(addrID, vertName, null, sourceMap.get("srcPort").toString(), null, source, "Situ");
									edgeArray.put(vertJSON);
									if (edgeArray.length() == pageSize) {
										return edgeArray;
									}
								}
							}
						}
						else {
							StringBuilder addrName = new StringBuilder();
							addrName.append(vertName);
							addrName.append(":");
							addrName.append(sourceMap.get("dstPort").toString());
							
							if (!addrSet.contains(addrName.toString())) {
								addrSet.add(addrName.toString());
								if (addrSet.size() >= (pageNumber * pageSize + 1)) {
									String addrID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
									vertJSON = GraphUtils.setAddressJson(addrID, vertName, null, sourceMap.get("dstPort").toString(), null, source, "Situ");
									edgeArray.put(vertJSON);
									if (edgeArray.length() == pageSize) {
										return edgeArray;
									}
								}
							}
						}
					}
				}
				
			} 
			else if (vertType.equalsIgnoreCase("port")) {
				
				Pattern p = Pattern.compile("^\\d+$");
				Matcher m = p.matcher(vertName);
				if (m.matches()) {
					Set<String> addrSet = new HashSet<String>(); 
					SearchResponse response = searchRequest.setQuery(QueryBuilders.boolQuery()
							.should(QueryBuilders.termQuery("srcPort", vertName))
							.should(QueryBuilders.termQuery("dstPort", vertName))).get();
					for (SearchHit hit : response.getHits().getHits()) {
						Map<String,Object> sourceMap = hit.getSource();
						if (sourceMap.get("srcPort").toString().equalsIgnoreCase(vertName)) {
							
							StringBuilder addrName = new StringBuilder();
							addrName.append(sourceMap.get("srcIP").toString());
							addrName.append(":");
							addrName.append(vertName);
							
							if (!addrSet.contains(addrName.toString())) {
								addrSet.add(addrName.toString());
								if (addrSet.size() >= (pageNumber * pageSize + 1)) {
									String addrID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
									vertJSON = GraphUtils.setAddressJson(addrID, sourceMap.get("srcIP").toString(), null, vertName, null, source, "Situ");
									edgeArray.put(vertJSON);
									if (edgeArray.length() == pageSize) {
										return edgeArray;
									}
								}
							}
						}
						else {
							StringBuilder addrName = new StringBuilder();
							addrName.append(sourceMap.get("dstIP").toString());
							addrName.append(":");
							addrName.append(vertName);
							
							if (!addrSet.contains(addrName.toString())) {
								addrSet.add(addrName.toString());
								if (addrSet.size() >= (pageNumber * pageSize + 1)) {
									String addrID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
									vertJSON = GraphUtils.setAddressJson(addrID, sourceMap.get("dstIP").toString(), null, vertName, null, source, "Situ");
									edgeArray.put(vertJSON);
									if (edgeArray.length() == pageSize) {
										return edgeArray;
									}
								}
							}
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
						SearchResponse response = searchRequest.setQuery(QueryBuilders.boolQuery()
								.should(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("srcIP", ip)).must(QueryBuilders.termQuery("srcPort", port)))
								.should(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("dstIP", ip)).must(QueryBuilders.termQuery("dstPort", port))))
								.setFrom(startIndex).setSize(pageSize).get();
						for (SearchHit hit : response.getHits().getHits()) {
							Map<String, Object> sourceMap = hit.getSource();
							String flowID = GraphUtils.buildString("stucco:Observable-", hit.getId());
							String srcAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							String dstAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							JSONObject flowSource = new JSONObject(hit.getSourceAsString());
							vertJSON = GraphUtils.setFlowJson(flowID, sourceMap.get("srcIP").toString(), sourceMap.get("srcPort").toString(), srcAddressID, sourceMap.get("dstIP").toString(), sourceMap.get("dstPort").toString(), dstAddressID, sourceMap.get("proto").toString(), source, "Situ", flowSource);
							edgeArray.put(vertJSON);
						}
					}
				}
				
			} 
			else {
				logger.warn("Unknown vertex type '" + vertType + "' encountered in getOutEdgesPage().");
			}
		}
		
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
					SearchResponse response = searchRequest.setQuery(QueryBuilders.boolQuery()
							.should(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("srcIP", ip)).must(QueryBuilders.termQuery("srcPort", port)))
							.should(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("dstIP", ip)).must(QueryBuilders.termQuery("dstPort", port))))
							.setSize(1).get();
					if (response.getHits().getTotalHits() >= 1) {
						String addressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
						vertJSON = GraphUtils.setAddressJson(addressID, ip, null, port, null, source, "Situ");
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
		int size = (pageNumber+1) * pageSize * 3;
		
		if (client != null) {
			if (vertType.equalsIgnoreCase("ip")) {
				Set<String> ipSet = new HashSet<String>();
				SearchResponse response = searchRequest.setSize(size).get();
				for (SearchHit hit : response.getHits().getHits()) {
					Map<String,Object> sourceMap = hit.getSource();
					if (!ipSet.contains(sourceMap.get("srcIP").toString())) {
						ipSet.add(sourceMap.get("srcIP").toString());
						if (ipSet.size() >= (pageNumber * pageSize + 1)) {
							String ipID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setIpJson(ipID, sourceMap.get("srcIP").toString(), source, "Situ");
							vertArray.put(vertJSON);
							if (vertArray.length() == pageSize) {
								return vertArray;
							}
						}
					}
					if (!ipSet.contains(sourceMap.get("dstIP").toString())) {
						ipSet.add(sourceMap.get("dstIP").toString());
						if (ipSet.size() >= (pageNumber * pageSize + 1)) {
							String ipID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setIpJson(ipID, sourceMap.get("dstIP").toString(), source, "Situ");
							vertArray.put(vertJSON);
							if (vertArray.length() == pageSize) {
								return vertArray;
							}
						}
					}
				}
			} 
			else if (vertType.equalsIgnoreCase("port")) {
				Set<String> portSet = new HashSet<String>();
				SearchResponse response = searchRequest.setSize(size).get();
				for (SearchHit hit : response.getHits().getHits()) {
					Map<String,Object> sourceMap = hit.getSource();
					if (!portSet.contains(sourceMap.get("srcPort").toString())) {
						portSet.add(sourceMap.get("srcPort").toString());
						if (portSet.size() >= (pageNumber * pageSize + 1)) {
							String portID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setIpJson(portID, sourceMap.get("srcPort").toString(), source, "Situ");
							vertArray.put(vertJSON);
							if (vertArray.length() == pageSize) {
								return vertArray;
							}
						}
					}
					if (!portSet.contains(sourceMap.get("dstPort").toString())) {
						portSet.add(sourceMap.get("dstPort").toString());
						if (portSet.size() >= (pageNumber * pageSize + 1)) {
							String portID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setIpJson(portID, sourceMap.get("dstPort").toString(), source, "Situ");
							vertArray.put(vertJSON);
							if (vertArray.length() == pageSize) {
								return vertArray;
							}
						}
					}
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

		return vertArray;
	}

	
	public JSONArray getVertsByConstraints(List<DBConstraint> constraints, int pageNumber, int pageSize) {
		return null;
	}

	
	long countVertsByConstraints(List<DBConstraint> constraints) {
		return -1;
	}
}