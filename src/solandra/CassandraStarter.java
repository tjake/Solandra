/**
 * 
 */
package solandra;

import lucandra.CassandraUtils;

/**
 * @author toddnine
 * 
 */
public class CassandraStarter {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		try {
			CassandraUtils.startup();

			JettySolandraRunner jetty = new JettySolandraRunner("/solandra",
					8983);
			jetty.start(false);
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

}
