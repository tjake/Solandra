/**
 * 
 */
package lucandra;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Iface;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.BeforeClass;
import org.junit.Ignore;

/**
 * @author Todd Nine
 * 
 */
@Ignore("Not a test, just a utility")
public class LucandraTestHelper {

	protected static IndexContext context;

	@BeforeClass
	public static void setupServer() throws Exception {

		TSocket socket = new TSocket(System.getProperty("cassandra.host", "127.0.0.1"), Integer.parseInt(System.getProperty("cassandra.port", "19160")));
		TTransport transport = new TFramedTransport(socket);
		TProtocol protocol = new TBinaryProtocol(transport);
		Cassandra.Client client = new Cassandra.Client(protocol);
		transport.open();
		

		String keyspace = System.getProperty("cassandra.keyspace", "Lucandra");

		List<KsDef> keyspaces = client.describe_keyspaces();

		boolean found = false;

		if (keyspaces != null) {
			for (KsDef ksDef : keyspaces) {
				if (ksDef.name.equals(keyspace)) {
					found = true;
					break;
				}
			}
		}

		if (!found) {
			
			List<CfDef> columns = new ArrayList<CfDef>();

			CfDef termInfo = new CfDef(keyspace, "TermInfo");
			termInfo.setComparator_type("BytesType");
			termInfo.setColumn_type("Super");
			termInfo.setSubcomparator_type("BytesType");

			columns.add(termInfo);

			CfDef documents = new CfDef(keyspace, "Documents");
			termInfo.setComparator_type("BytesType");

			columns.add(documents);
			
			KsDef keyspaceDefinition = new KsDef(keyspace,
					"org.apache.cassandra.locator.SimpleStrategy", 1,
					columns);
			
			client.system_add_keyspace(keyspaceDefinition);
			
			transport.flush();
			transport.close();
			
		} 
		
		//create the realy connection
		
		Iface testConnection = CassandraUtils.createConnection();

		context = new IndexContext(testConnection, ConsistencyLevel.ONE);

	}

	

}
