package gov.ornl.stucco.elasticsearch;

import java.util.Map;

import java.io.FileNotFoundException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;


/**
 * Sapphire Elasticsearch index connection
 */
public class ElasticsearchDBConnection {
	private static String CLUSTER = "elasticsearch";
	private static String HOST = "localhost";
	private static int PORT = 9300;

	private PreBuiltTransportClient prebuiltClient = null;
	private Settings settings = null;
	private InetSocketTransportAddress inetAddress = null;


	public ElasticsearchDBConnection() throws UnknownHostException {
		prebuildClient();
	} 

	public ElasticsearchDBConnection(String configFile) throws UnknownHostException, FileNotFoundException {
		setConnectionInfo(configFile);
		prebuildClient();
	}

	private void setConnectionInfo(String configFile) throws UnknownHostException, FileNotFoundException {
		ConfigLoader loader = new ConfigLoader(configFile);
		Map<String, Object> config = loader.getConfig("elasticsearch_connection");

		CLUSTER = config.get("cluster").toString();
		HOST = config.get("host").toString();
		PORT = (Integer)config.get("port");
	}

	private void prebuildClient() throws UnknownHostException {
		Settings settings = Settings.builder().put("cluster.name", CLUSTER).build();
		prebuiltClient = new PreBuiltTransportClient(settings);
		inetAddress = new InetSocketTransportAddress(InetAddress.getByName(HOST), PORT);
	}

	public Connection getConnection() {
		return new Connection(prebuiltClient, inetAddress);
	}
}
