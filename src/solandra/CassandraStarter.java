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

		String context = System.getProperty("solandra.context", "/solandra");
		
		int port = Integer.parseInt(System.getProperty("solandra.port", "8983"));
		
		try {
			CassandraUtils.startup();

			JettySolandraRunner jetty = new JettySolandraRunner(context, port);
			jetty.start(false);
		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

}
