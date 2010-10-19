/**
 * 
 */
package lucandra;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra.Iface;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.junit.BeforeClass;

/**
 * @author Todd Nine
 * 
 */
public class LucandraTestHelper {

	protected static IndexContext context;

	@BeforeClass
	public static void setupServer() throws Exception {

		Iface client = CassandraUtils.createConnection();

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
			KsDef keyspaceDefinition = new KsDef(keyspace,
					"org.apache.cassandra.locator.SimpleStrategy", 1,
					getColumns(keyspace));

			client.system_update_keyspace(keyspaceDefinition);

		} else {

			for (CfDef cf : getColumns(keyspace)) {
				client.system_update_column_family(cf);
			}
		}

		context = new IndexContext(client, "Lucandra", ConsistencyLevel.ONE);

	}

	/**
	 * 
	 * @return
	 */
	private static List<CfDef> getColumns(String keyspace) {
		List<CfDef> columns = new ArrayList<CfDef>();

		CfDef termInfo = new CfDef(keyspace, "TermInfo");
		termInfo.setComparator_type("BytesType");
		termInfo.setColumn_type("Super");
		termInfo.setSubcomparator_type("BytesType");
		termInfo.setKey_cache_size(1000);

		columns.add(termInfo);

		CfDef documents = new CfDef(keyspace, "Documents");
		termInfo.setComparator_type("BytesType");
		termInfo.setKey_cache_size(1000);

		columns.add(documents);

		return columns;
	}

}
