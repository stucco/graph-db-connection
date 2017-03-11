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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

import gov.ornl.stucco.utils.GraphUtils;
import gov.pnnl.stucco.dbconnect.Condition;
import gov.pnnl.stucco.dbconnect.DBConstraint; 

public class Connection {
	private TransportClient prebuiltClient = null;
	private InetSocketTransportAddress inetAddress = null;
	private TransportClient client = null;
	private Logger logger;		
	private Set<String> sourceSet;
	private String indexName;
	
	private static Pattern portPattern = Pattern.compile("^\\d+$");
	private static Pattern ipPattern = Pattern.compile("^(\\d{1,3}\\.){3}\\d{1,3}$");
	private static Pattern addressPattern = Pattern.compile("^(\\d{1,3}\\.){3}\\d{1,3}:\\d+$");
	private static Pattern flowPattern = Pattern.compile("^(\\d{1,3}\\.){3}\\d{1,3}:\\d+_through_(\\d{1,3}\\.){3}\\d{1,3}:\\d+$");
	
	public Connection(TransportClient prebuiltClient, InetSocketTransportAddress inetAddress, String index, Set<String> sources, Logger logger) {
		this.prebuiltClient = prebuiltClient;
		this.inetAddress = inetAddress;
		this.logger = logger;
		this.indexName = index;
		this.sourceSet = sources;
	}

	
	public void open() throws UnknownHostException {
  		client = prebuiltClient.addTransportAddress(inetAddress);
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
				
				Matcher m = flowPattern.matcher(vertName);
				if (m.matches()) {
					String[] flowPieces = vertName.split("_through_");
					String[] srcAddr = flowPieces[0].split(":");
					String[] dstAddr = flowPieces[1].split(":");
					if ((srcAddr.length == 2) && (dstAddr.length == 2)) {
						String srcIP = srcAddr[0];
						String srcPort = srcAddr[1];
						String dstIP = dstAddr[0];
						String dstPort = dstAddr[1];
						SearchResponse response = client.prepareSearch(indexName).setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("srcIP", srcIP)).must(QueryBuilders.termQuery("srcPort", srcPort)).must(QueryBuilders.termQuery("dstIP", dstIP)).must(QueryBuilders.termQuery("dstPort", dstPort))).setSize(1).get();
						if (response.getHits().getTotalHits() >= 1) {
							String srcAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setAddressJson(srcAddressID, srcIP, null, srcPort, null, sourceSet, "Situ");
							edgeArray.put(vertJSON);
							String dstAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setAddressJson(dstAddressID, dstIP, null, dstPort, null, sourceSet, "Situ");
							edgeArray.put(vertJSON);
						}
					}
				}
				
			} 
			else if (vertType.equalsIgnoreCase("address")) {
				
				Matcher m = addressPattern.matcher(vertName);
				if (m.matches()) {
					String[] addressPieces = vertName.split(":");
					if (addressPieces.length == 2) {
						String ip = addressPieces[0];
						String port = addressPieces[1];
						SearchResponse response = client.prepareSearch(indexName).setQuery(QueryBuilders.boolQuery()
								.should(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("srcIP", ip)).must(QueryBuilders.termQuery("srcPort", port)))
								.should(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("dstIP", ip)).must(QueryBuilders.termQuery("dstPort", port))))
								.setSize(1).get();
						if (response.getHits().getTotalHits() >= 1) {
							String ipID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setIpJson(ipID, ip, sourceSet, "Situ");
							edgeArray.put(vertJSON);
							String portID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setPortJson(portID, port, sourceSet, "Situ");
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
				
				Matcher m = ipPattern.matcher(vertName);
				if (m.matches()) {
					Set<String> addrSet = new HashSet<String>();
					SearchResponse response = client.prepareSearch(indexName).setQuery(QueryBuilders.boolQuery()
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
									vertJSON = GraphUtils.setAddressJson(addrID, vertName, null, sourceMap.get("srcPort").toString(), null, sourceSet, "Situ");
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
									vertJSON = GraphUtils.setAddressJson(addrID, vertName, null, sourceMap.get("dstPort").toString(), null, sourceSet, "Situ");
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
				
				Matcher m = portPattern.matcher(vertName);
				if (m.matches()) {
					Set<String> addrSet = new HashSet<String>(); 
					SearchResponse response = client.prepareSearch(indexName).setQuery(QueryBuilders.boolQuery()
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
									vertJSON = GraphUtils.setAddressJson(addrID, sourceMap.get("srcIP").toString(), null, vertName, null, sourceSet, "Situ");
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
									vertJSON = GraphUtils.setAddressJson(addrID, sourceMap.get("dstIP").toString(), null, vertName, null, sourceSet, "Situ");
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
				
				Matcher m = addressPattern.matcher(vertName);
				if (m.matches()) {
					String[] addressPieces = vertName.split(":");
					if (addressPieces.length == 2) {
						String ip = addressPieces[0];
						String port = addressPieces[1];
						SearchResponse response = client.prepareSearch(indexName).setQuery(QueryBuilders.boolQuery()
								.should(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("srcIP", ip)).must(QueryBuilders.termQuery("srcPort", port)))
								.should(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("dstIP", ip)).must(QueryBuilders.termQuery("dstPort", port))))
								.setFrom(startIndex).setSize(pageSize).get();
						for (SearchHit hit : response.getHits().getHits()) {
							Map<String, Object> sourceMap = hit.getSource();
							String flowID = GraphUtils.buildString("stucco:Observable-", hit.getId());
							String srcAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							String dstAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							JSONObject flowSource = new JSONObject(hit.getSourceAsString());
							vertJSON = GraphUtils.setFlowJson(flowID, sourceMap.get("srcIP").toString(), sourceMap.get("srcPort").toString(), srcAddressID, sourceMap.get("dstIP").toString(), sourceMap.get("dstPort").toString(), dstAddressID, sourceMap.get("proto").toString(), sourceSet, "Situ", flowSource);
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
			
			Matcher m = portPattern.matcher(vertName);
			if (m.matches()) {				
				SearchResponse response = client.prepareSearch(indexName).setQuery(QueryBuilders.multiMatchQuery((String)vertName, "srcPort", "dstPort")).setSize(1).get();
				long totalHits = response.getHits().getTotalHits();
				if (totalHits >= 1) {
					String portID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
					vertJSON = GraphUtils.setPortJson(portID, vertName, sourceSet, "Situ");
				}
				return vertJSON;
			}
			
			m = ipPattern.matcher(vertName);
			if (m.matches()) {
				SearchResponse response = client.prepareSearch(indexName).setQuery(QueryBuilders.multiMatchQuery((String)vertName, "srcIP", "dstIP")).setSize(1).get();
				long totalHits = response.getHits().getTotalHits();
				if (totalHits >= 1) {
					String ipID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
					vertJSON = GraphUtils.setIpJson(ipID, vertName, sourceSet, "Situ");
				}
				return vertJSON;
			}
			
			m = addressPattern.matcher(vertName);
			if (m.matches()) {
				String[] addressPieces = vertName.split(":");
				if (addressPieces.length == 2) {
					String ip = addressPieces[0];
					String port = addressPieces[1];
					SearchResponse response = client.prepareSearch(indexName).setQuery(QueryBuilders.boolQuery()
							.should(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("srcIP", ip)).must(QueryBuilders.termQuery("srcPort", port)))
							.should(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("dstIP", ip)).must(QueryBuilders.termQuery("dstPort", port))))
							.setSize(1).get();
					if (response.getHits().getTotalHits() >= 1) {
						String addressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
						vertJSON = GraphUtils.setAddressJson(addressID, ip, null, port, null, sourceSet, "Situ");
					}
					return vertJSON;
				}
			}
			
			m = flowPattern.matcher(vertName);
			if (m.matches()) {
				String[] flowPieces = vertName.split("_through_");
				String[] srcAddr = flowPieces[0].split(":");
				String[] dstAddr = flowPieces[1].split(":");
				if ((srcAddr.length == 2) && (dstAddr.length == 2)) {
					String srcIP = srcAddr[0];
					String srcPort = srcAddr[1];
					String dstIP = dstAddr[0];
					String dstPort = dstAddr[1];
					SearchResponse response = client.prepareSearch(indexName).setQuery(QueryBuilders.boolQuery()
							.must(QueryBuilders.termQuery("srcIP", srcIP))
							.must(QueryBuilders.termQuery("srcPort", srcPort))
							.must(QueryBuilders.termQuery("dstIP", dstIP))
							.must(QueryBuilders.termQuery("dstPort", dstPort))).setSize(1).get();
					long totalHits = response.getHits().getTotalHits();
					if (totalHits >= 1) {
						SearchHit result = response.getHits().getHits()[0];
						Map<String, Object> sourceMap = result.getSource();
						String flowID = GraphUtils.buildString("stucco:Observable-", result.getId());
						String srcAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
						String dstAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
						JSONObject flowSource = new JSONObject(result.getSourceAsString());
						vertJSON = GraphUtils.setFlowJson(flowID, srcIP, srcPort, srcAddressID, dstIP, dstPort, dstAddressID, sourceMap.get("proto").toString(), sourceSet, "Situ", flowSource);
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
		int size = (pageNumber + 1) * pageSize * 3;
		
		if (client != null) {
			if (vertType.equalsIgnoreCase("ip")) {
				Set<String> ipSet = new HashSet<String>();
				SearchResponse response = client.prepareSearch(indexName).setSize(size).get();
				for (SearchHit hit : response.getHits().getHits()) {
					Map<String,Object> sourceMap = hit.getSource();
					if (!ipSet.contains(sourceMap.get("srcIP").toString())) {
						ipSet.add(sourceMap.get("srcIP").toString());
						if (ipSet.size() >= (pageNumber * pageSize + 1)) {
							String ipID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setIpJson(ipID, sourceMap.get("srcIP").toString(), sourceSet, "Situ");
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
							vertJSON = GraphUtils.setIpJson(ipID, sourceMap.get("dstIP").toString(), sourceSet, "Situ");
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
				SearchResponse response = client.prepareSearch(indexName).setSize(size).get();
				for (SearchHit hit : response.getHits().getHits()) {
					Map<String,Object> sourceMap = hit.getSource();
					if (!portSet.contains(sourceMap.get("srcPort").toString())) {
						portSet.add(sourceMap.get("srcPort").toString());
						if (portSet.size() >= (pageNumber * pageSize + 1)) {
							String portID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setPortJson(portID, sourceMap.get("srcPort").toString(), sourceSet, "Situ");
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
							vertJSON = GraphUtils.setPortJson(portID, sourceMap.get("dstPort").toString(), sourceSet, "Situ");
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
				SearchResponse response = client.prepareSearch(indexName).setSize(size).get();
				SearchHit[] hits = response.getHits().getHits();
				for (SearchHit hit : hits) {
					Map<String,Object> sourceMap = hit.getSource();
					StringBuilder sb = new StringBuilder();
					sb.append(sourceMap.get("srcIP").toString());
					sb.append(":");
					sb.append(sourceMap.get("srcPort").toString());
					if (!addrSet.contains(sb.toString())) {
						addrSet.add(sb.toString());
						if (addrSet.size() >= (pageNumber * pageSize +1)) {
							String addrID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setAddressJson(addrID, sourceMap.get("srcIP").toString(), null, sourceMap.get("srcPort").toString(), null, sourceSet, "Situ");
							vertArray.put(vertJSON);
							if (vertArray.length() == pageSize) {
								return vertArray;
							}
						}
					}
					
					sb = new StringBuilder();
					sb.append(sourceMap.get("dstIP").toString());
					sb.append(":");
					sb.append(sourceMap.get("dstPort").toString());
					if (!addrSet.contains(sb.toString())) {
						addrSet.add(sb.toString());
						if (addrSet.size() >= (pageNumber * pageSize +1)) {
							String addrID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setAddressJson(addrID, sourceMap.get("dstIP").toString(), null, sourceMap.get("dstPort").toString(), null, sourceSet, "Situ");
							vertArray.put(vertJSON);
							if (vertArray.length() == pageSize) {
								return vertArray;
							}
						}
					}
				}
				
			} 
			else if (vertType.equalsIgnoreCase("flow")) {
				SearchResponse response = client.prepareSearch(indexName).setSize(size).get();
				SearchHit[] hits = response.getHits().getHits();
				
				for (SearchHit hit : hits) {
					Map<String, Object> sourceMap = hit.getSource();
					String flowID = GraphUtils.buildString("stucco:Observable-", hit.getId());
					String srcAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
					String dstAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
					JSONObject flowSource = new JSONObject(hit.getSourceAsString());
					vertJSON = GraphUtils.setFlowJson(flowID, (sourceMap.get("srcIP").toString()), (sourceMap.get("srcPort").toString()), srcAddressID, (sourceMap.get("dstIP").toString()), (sourceMap.get("dstPort").toString()), dstAddressID, (sourceMap.get("proto").toString()), sourceSet, "Situ", flowSource);
					vertArray.put(vertJSON);
				}
								
			} 
			else if (vertType.equalsIgnoreCase("observable")) {
				Set<String> ipSet = new HashSet<String>();
				Set<String> portSet = new HashSet<String>();
				Set<String> addrSet = new HashSet<String>();
				int totalVertsSeen = 0;
				
				SearchResponse response = client.prepareSearch(indexName).setSize(size).get();
				SearchHit[] hits = response.getHits().getHits();
				for (SearchHit hit : hits) {
					Map<String,Object> sourceMap = hit.getSource();
					
					
					//IPs
					if (!ipSet.contains(sourceMap.get("srcIP").toString())) {
						ipSet.add(sourceMap.get("srcIP").toString());
						totalVertsSeen ++;
						if (totalVertsSeen >= (pageNumber * pageSize + 1)) {
							String ipID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setIpJson(ipID, sourceMap.get("srcIP").toString(), sourceSet, "Situ");
							vertArray.put(vertJSON);
							if (vertArray.length() == pageSize) {
								return vertArray;
							}
						}
					}
					if (!ipSet.contains(sourceMap.get("dstIP").toString())) {
						ipSet.add(sourceMap.get("dstIP").toString());
						totalVertsSeen ++;
						if (totalVertsSeen >= (pageNumber * pageSize + 1)) {
							String ipID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setIpJson(ipID, sourceMap.get("dstIP").toString(), sourceSet, "Situ");
							vertArray.put(vertJSON);
							if (vertArray.length() == pageSize) {
								return vertArray;
							}
						}
					}
					
					//Ports
					if (!portSet.contains(sourceMap.get("srcPort").toString())) {
						portSet.add(sourceMap.get("srcPort").toString());
						totalVertsSeen ++;
						if (totalVertsSeen >= (pageNumber * pageSize + 1)) {
							String portID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setPortJson(portID, sourceMap.get("srcPort").toString(), sourceSet, "Situ");
							vertArray.put(vertJSON);
							if (vertArray.length() == pageSize) {
								return vertArray;
							}
						}
					}
					if (!portSet.contains(sourceMap.get("dstPort").toString())) {
						portSet.add(sourceMap.get("dstPort").toString());
						totalVertsSeen ++;
						if (totalVertsSeen >= (pageNumber * pageSize + 1)) {
							String portID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setPortJson(portID, sourceMap.get("dstPort").toString(), sourceSet, "Situ");
							vertArray.put(vertJSON);
							if (vertArray.length() == pageSize) {
								return vertArray;
							}
						}
					}
					
					//Addresses
					StringBuilder sb = new StringBuilder();
					sb.append(sourceMap.get("srcIP").toString());
					sb.append(":");
					sb.append(sourceMap.get("srcPort").toString());
					if (!addrSet.contains(sb.toString())) {
						addrSet.add(sb.toString());
						totalVertsSeen ++;
						if (totalVertsSeen >= (pageNumber * pageSize + 1)) {
							String addrID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setAddressJson(addrID, sourceMap.get("srcIP").toString(), null, sourceMap.get("srcPort").toString(), null, sourceSet, "Situ");
							vertArray.put(vertJSON);
							if (vertArray.length() == pageSize) {
								return vertArray;
							}
						}
					}
					sb = new StringBuilder();
					sb.append(sourceMap.get("dstIP").toString());
					sb.append(":");
					sb.append(sourceMap.get("dstPort").toString());
					if (!addrSet.contains(sb.toString())) {
						addrSet.add(sb.toString());
						totalVertsSeen ++;
						if (totalVertsSeen >= (pageNumber * pageSize + 1)) {
							String addrID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
							vertJSON = GraphUtils.setAddressJson(addrID, sourceMap.get("dstIP").toString(), null, sourceMap.get("dstPort").toString(), null, sourceSet, "Situ");
							vertArray.put(vertJSON);
							if (vertArray.length() == pageSize) {
								return vertArray;
							}
						}
					}
					
					//Flows
					totalVertsSeen ++;
					if (totalVertsSeen >= (pageNumber * pageSize + 1)) {
						String flowID = GraphUtils.buildString("stucco:Observable-", hit.getId());
						String srcAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
						String dstAddressID = GraphUtils.buildString("stucco:Observable-", UUID.randomUUID());
						JSONObject flowSource = new JSONObject(hit.getSourceAsString());
						vertJSON = GraphUtils.setFlowJson(flowID, (sourceMap.get("srcIP").toString()), (sourceMap.get("srcPort").toString()), srcAddressID, (sourceMap.get("dstIP").toString()), (sourceMap.get("dstPort").toString()), dstAddressID, (sourceMap.get("proto").toString()), sourceSet, "Situ", flowSource);
						vertArray.put(vertJSON);
						if (vertArray.length() == pageSize) {
							return vertArray;
						}
					}
					
				}
				
			}
			else {
				logger.warn("Unknown vertex type '" + vertType + "' encountered in getVertByType().");
			}
		}

		return vertArray;
	}

	
	public JSONArray getVertsByConstraints(List<DBConstraint> constraints, int pageNumber, int pageSize) {
		JSONArray vertArray = new JSONArray();
		JSONArray resultArray = new JSONArray();
		Set<String> dismissSet = new HashSet<String>();
		
		if (client != null) {
			
			for (DBConstraint constraint : constraints) {
				if (constraint.getProp().equalsIgnoreCase("name")) {
					String vertName = constraint.getVal().toString();
					if (constraint.getCond().equals(Condition.eq)) {
						resultArray.put(getVertByName(vertName));
					}
					else if (constraint.getCond().equals(Condition.neq)) {
						dismissSet.add(vertName);
					}
				}
				else if ((constraint.getProp().equalsIgnoreCase("vertexType")) || (constraint.getProp().equalsIgnoreCase("observableType"))) {
					String vertType = constraint.getVal().toString();
					resultArray = getVertByType(vertType, pageNumber, (pageSize + constraints.size()));
				}
			}
			
			for (int i=0; i<resultArray.length(); i++) {
				String key = ((JSONObject)resultArray.get(i)).get("name").toString();
				if ((!dismissSet.contains(key)) && (vertArray.length() < pageSize)) {
					vertArray.put(resultArray.get(i));
				}
			}
			
		}
		
		return vertArray;
	}

	
	public long countVertsByConstraints(List<DBConstraint> constraints) {
		long totalVerts = 0;
		int maxSize = 3000;
		if (client != null) {
		
			for (DBConstraint constraint : constraints) {
				
				if ((constraint.getProp().equalsIgnoreCase("vertexType")) || (constraint.getProp().equalsIgnoreCase("observableType"))) {
					if (constraint.getVal().toString().equalsIgnoreCase("ip")) {
						if (constraint.getCond().equals(Condition.eq)) {
							//all ips
							SearchResponse response = client.prepareSearch(indexName).addAggregation(AggregationBuilders.terms("srcIPs").field("srcIP").size(maxSize)).addAggregation(AggregationBuilders.terms("dstIPs").field("dstIP").size(maxSize)).get();
							Terms agTerms = response.getAggregations().get("srcIPs");
							List<Bucket> srcBuckets = agTerms.getBuckets();
							agTerms = response.getAggregations().get("dstIPs");
							List<Bucket> dstBuckets = agTerms.getBuckets();
							totalVerts += srcBuckets.size() + dstBuckets.size();
							for (Bucket bucket : srcBuckets) {
								if (dstBuckets.contains(bucket)) {
									totalVerts --;
								}
							}
						}
					}
					else if (constraint.getVal().toString().equalsIgnoreCase("port")) {
						if (constraint.getCond().equals(Condition.eq)) {
							//all ports
							SearchResponse response = client.prepareSearch(indexName).addAggregation(AggregationBuilders.terms("srcPorts").field("srcPort").size(maxSize))
									.addAggregation(AggregationBuilders.terms("dstPorts").field("dstPort").size(maxSize))
									.get();
							Terms agTerms = response.getAggregations().get("srcPorts");
							List<Bucket> srcBuckets = agTerms.getBuckets();
							agTerms = response.getAggregations().get("dstPorts");
							List<Bucket> dstBuckets = agTerms.getBuckets();
							totalVerts += srcBuckets.size() + dstBuckets.size();
							for (Bucket bucket : srcBuckets) {
								if (dstBuckets.contains(bucket)) {
									totalVerts --;
								}
							}
						}
					}
					else if (constraint.getVal().toString().equalsIgnoreCase("address")) {
						if (constraint.getCond().equals(Condition.eq)) {
							//all addresses
							JSONArray vertArray = getVertByType("address", 0, maxSize);
							totalVerts += vertArray.length();
						}
					}
					else if (constraint.getVal().toString().equalsIgnoreCase("flow")) {
						if (constraint.getCond().equals(Condition.eq)) {
							//all flows
							SearchResponse response = client.prepareSearch(indexName).get();
							totalVerts += response.getHits().getTotalHits();
						}
					}
					else if (constraint.getVal().toString().equalsIgnoreCase("observable")) {
						if (constraint.getCond().equals(Condition.eq)) {
							//all ips, ports, addresses, flows
							SearchResponse response = client.prepareSearch(indexName).addAggregation(AggregationBuilders.terms("srcIPs").field("srcIP").size(maxSize))
									.addAggregation(AggregationBuilders.terms("dstIPs").field("dstIP").size(maxSize))
									.addAggregation(AggregationBuilders.terms("srcPorts").field("srcPort").size(maxSize))
									.addAggregation(AggregationBuilders.terms("dstPorts").field("dstPort").size(maxSize))
									.get();
							Terms agTerms = response.getAggregations().get("srcIPs");
							List<Bucket> srcBuckets = agTerms.getBuckets();
							agTerms = response.getAggregations().get("dstIPs");
							List<Bucket> dstBuckets = agTerms.getBuckets();
							totalVerts += srcBuckets.size() + dstBuckets.size();
							for (Bucket bucket : srcBuckets) {
								if (dstBuckets.contains(bucket)) {
									totalVerts --;
								}
							}
	
							agTerms = response.getAggregations().get("srcPorts");
							srcBuckets = agTerms.getBuckets();
							agTerms = response.getAggregations().get("dstPorts");
							dstBuckets = agTerms.getBuckets();
							totalVerts += srcBuckets.size() + dstBuckets.size();
							for (Bucket bucket : srcBuckets) {
								if (dstBuckets.contains(bucket)) {
									totalVerts --;
								}
							}
							
							JSONArray vertArray = getVertByType("address", 0, maxSize);
							totalVerts += vertArray.length();
							
							totalVerts += response.getHits().getTotalHits();
						}
						else if (constraint.getCond().equals(Condition.neq)) {
							//nothing
							totalVerts = 0;
							return totalVerts;
						}
					}
					else {
						logger.warn("Unknown constraint '" + constraint.getProp() + "' '" + constraint.getCond().toString() + "' '" + constraint.getVal().toString() + "' encountered in countVertsByConstraints().");
					}
			
				}
				else if (constraint.getProp().equalsIgnoreCase("name")) {
					if (constraint.getCond().equals(Condition.eq)) {
						//only one vertex for a given ip, port, address, or flow name
						totalVerts = 1;
						return totalVerts;
					}
					else if (constraint.getCond().equals(Condition.neq)) {
						//all vertices except one
						String vertName = constraint.getVal().toString();
						Matcher m = portPattern.matcher(vertName);
						if (m.matches()) {				
							SearchResponse response = client.prepareSearch(indexName).setQuery(QueryBuilders.multiMatchQuery((String)vertName, "srcPort", "dstPort")).setSize(0).get();
							if (response.getHits().getTotalHits() >= 1) {
								totalVerts --;
							}
						}
						
						m = ipPattern.matcher(vertName);
						if (m.matches()) {				
							SearchResponse response = client.prepareSearch(indexName).setQuery(QueryBuilders.multiMatchQuery((String)vertName, "srcIP", "dstIP")).setSize(0).get();
							if (response.getHits().getTotalHits() >= 1) {
								totalVerts --;
							}
						}
						
						m = addressPattern.matcher(vertName);
						if (m.matches()) {
							String[] addressPieces = vertName.split(":");
							if (addressPieces.length == 2) {
								String ip = addressPieces[0];
								String port = addressPieces[1];
								SearchResponse response = client.prepareSearch(indexName).setQuery(QueryBuilders.boolQuery()
										.should(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("srcIP", ip)).must(QueryBuilders.termQuery("srcPort", port)))
										.should(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("dstIP", ip)).must(QueryBuilders.termQuery("dstPort", port))))
										.setSize(0).get();
								if (response.getHits().getTotalHits() >= 1) {
									totalVerts --;
								}
							}
						}
						
						m = flowPattern.matcher(vertName);
						if (m.matches()) {
							String[] flowPieces = vertName.split("_through_");
							String[] srcAddr = flowPieces[0].split(":");
							String[] dstAddr = flowPieces[1].split(":");
							if ((srcAddr.length == 2) && (dstAddr.length == 2)) {
								String srcIP = srcAddr[0];
								String srcPort = srcAddr[1];
								String dstIP = dstAddr[0];
								String dstPort = dstAddr[1];
								SearchResponse response = client.prepareSearch(indexName).setQuery(QueryBuilders.boolQuery()
										.must(QueryBuilders.termQuery("srcIP", srcIP))
										.must(QueryBuilders.termQuery("srcPort", srcPort))
										.must(QueryBuilders.termQuery("dstIP", dstIP))
										.must(QueryBuilders.termQuery("dstPort", dstPort))).setSize(0).get();
								if (response.getHits().getTotalHits() >= 1) {
									totalVerts --;
								}
							}
						}
					}
				}
				else {
					logger.warn("Unknown constraint '" + constraint.getProp() + "' '" + constraint.getCond().toString() + "' '" + constraint.getVal().toString() + "' encountered in countVertsByConstraints().");
				}
					
			}
			
		}
		
		return totalVerts;
	}
}