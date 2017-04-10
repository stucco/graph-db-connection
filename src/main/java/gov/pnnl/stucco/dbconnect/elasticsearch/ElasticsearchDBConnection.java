package gov.pnnl.stucco.dbconnect.elasticsearch;

import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.pnnl.stucco.dbconnect.DBConnectionAlignment;
import gov.pnnl.stucco.dbconnect.DBConnectionFactory;


/**
 * Sapphire Elasticsearch index connection
 */
public class ElasticsearchDBConnection {
	private static String CLUSTER = "elasticsearch";
	private static String HOST = "localhost";
	private static int PORT = 9300;
	private static String INDEX = "situ";
	private static Set<String> SOURCESET = new HashSet<String>();

	private TransportClient prebuiltClient = null;
	private Settings settings = null;
	private InetSocketTransportAddress inetAddress = null;
	private static Logger logger = null;


	public ElasticsearchDBConnection() throws UnknownHostException {
		prebuildClient();
		logger = LoggerFactory.getLogger(ElasticsearchDBConnection.class);
	} 

	public ElasticsearchDBConnection(String configFile) throws UnknownHostException, FileNotFoundException {
		setConnectionInfo(configFile);
		prebuildClient();
		logger = LoggerFactory.getLogger(ElasticsearchDBConnection.class);
	}

	private void setConnectionInfo(String configFile) throws UnknownHostException, FileNotFoundException {
		ConfigLoader loader = new ConfigLoader(configFile);
		Map<String, Object> config = loader.getConfig("elasticsearch_connection");

		CLUSTER = config.get("cluster").toString();
		HOST = config.get("host").toString();
		PORT = (Integer)config.get("port");
		INDEX = config.get("index").toString();
		List<String> sources = (ArrayList)config.get("source");
		SOURCESET.addAll(sources);
	}

	private void prebuildClient() throws UnknownHostException {
		settings = Settings.builder().put("cluster.name", CLUSTER).build();
		prebuiltClient = new PreBuiltTransportClient(settings);
		inetAddress = new InetSocketTransportAddress(InetAddress.getByName(HOST), PORT);
	}

	public Connection getConnection() throws UnknownHostException {
		Connection con = new Connection(prebuiltClient, inetAddress, INDEX, SOURCESET, logger);
		con.setStuccoDBConnection(getStuccoDB());
		con.open();

		return con;
	}

  public DBConnectionAlignment getStuccoDB() {
    String type = System.getenv("STUCCO_DB_TYPE");
    if (type == null) {
        throw (new NullPointerException("Missing environment variable STUCCO_DB_TYPE"));
    } 

    String config = System.getenv("STUCCO_DB_CONFIG");
	  if (config == null) {
	      throw (new NullPointerException("Missing environment variable STUCCO_DB_CONFIG"));
	  }
	  DBConnectionFactory factory = DBConnectionFactory.getFactory(DBConnectionFactory.Type.valueOf(type));
	  factory.setConfiguration(config);
	  DBConnectionAlignment db = factory.getDBConnectionTestInterface();

	  return db;
  }
}
