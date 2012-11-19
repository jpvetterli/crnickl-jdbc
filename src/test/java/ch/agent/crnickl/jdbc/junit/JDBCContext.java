package ch.agent.crnickl.jdbc.junit;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.util.Map;

import org.hsqldb.cmdline.SqlFile;

import ch.agent.crnickl.api.SimpleDatabaseManager;
import ch.agent.crnickl.jdbc.JDBCDatabase;

public class JDBCContext extends ch.agent.crnickl.junit.Context {

	private static final String DDL_RESOURCE = "feature.DDL";
	
	private static class Singleton {
		private static JDBCContext instance = new JDBCContext();
	}	
	
	public static JDBCContext getInstance() {
		return Singleton.instance;
	}
	
	@Override
	protected void setup(SimpleDatabaseManager dbm, Map<String, String> parameters) throws Exception {
		String DDL = parameters.get(DDL_RESOURCE);
		if (DDL == null)
			throw new RuntimeException("Resource " + DDL_RESOURCE + " not found.");
		Connection c = ((JDBCDatabase)dbm.getDatabase()).getConnection();
		sql(c, DDL);
		c.commit();

		super.setup(dbm, parameters);
	}

	private static void sql(Connection c, String resource) throws Exception {
		InputStream inputStream = ClassLoader.getSystemClassLoader().getResourceAsStream(resource);
		if (inputStream == null)
		 	inputStream = new FileInputStream(resource);
		SqlFile sqlf = new SqlFile(new InputStreamReader(inputStream), resource, null, null, false, null);
		sqlf.setConnection(c);
		sqlf.execute();
	}
	
}
