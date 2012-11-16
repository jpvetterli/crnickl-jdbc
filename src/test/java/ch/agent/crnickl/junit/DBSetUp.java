/*
 *   Copyright 2012 Hauser Olsson GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Package: ch.agent.crnickl.junit
 * Type: DBSetUp
 * Version: 1.0.1
 */
package ch.agent.crnickl.junit;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import org.hsqldb.cmdline.SqlFile;

import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.SimpleDatabaseManager;
import ch.agent.crnickl.api.UpdatableValueType;
import ch.agent.crnickl.api.ValueType;
import ch.agent.crnickl.jdbc.JDBCDatabase;

public class DBSetUp {
	
	private static final String DDL = "sql/HyperSQL_DDL_base.sql";
	public static boolean MEMORY_DB = true;
	public static boolean STANDARD_REGEXP = false;
	public static String MYSQL_PARM_FILE = null;
	// many test cases do not work well against existing database because of "leftovers"
	// but the setup is okay with this file for example: "/home/jp/A-WORK/tsm/tsm-JDBC-3312.parm";
	
	/**
	 * About session$jdbcUrl2.
	 * <p>
	 * To run test against a file based HSQL, start the server 
	 * using a command like (all on one line):
	 * <blockquote>
	 * <xmp>$ java -cp /usr/local/share/java/hsqldb-2.2.8/hsqldb.jar org.hsqldb.server.Server ...
	 *   --database.0 file:/home/jp/HSQL/DB2/test2 --dbname.0 test2 &
	 * </xmp>
	 * </blockquote>
	 * To browse the database:
	 * <blockquote>
	 * <xmp>java -cp /usr/local/share/java/hsqldb-2.2.8/hsqldb.jar org.hsqldb.util.DatabaseManagerSwing &
	 * </xmp>
	 */
	public static String dbName = "bt";
	public static String dbClass = ch.agent.crnickl.jdbc.JDBCDatabase.class.getName();
	public static String dbStrictNameSpace = "true"; // with "false" cannot run with an empty database
	public static String session$jdbcDriver = "org.hsqldb.jdbc.JDBCDriver";
	public static String session$jdbcUrl1 = "jdbc:hsqldb:mem:testdb";
	public static String session$jdbcUrl2 = "jdbc:hsqldb:hsql://localhost/test2";
	public static String session$db = ""; // leave empty when no %s place holder in jdbcUrl
	public static String session$user = "sa";
	public static String session$password = "";
	
	private static Database db;
	private static SimpleDatabaseManager dbm;
	private static boolean error;

	private static void sql(Connection c, String resource) throws Exception {
		InputStream inputStream = ClassLoader.getSystemClassLoader().getResourceAsStream(resource);
		if (inputStream == null)
		 	inputStream = new FileInputStream(resource);
		SqlFile sqlf = new SqlFile(new InputStreamReader(inputStream), resource, null, null, false, null);
		sqlf.setConnection(c);
		sqlf.execute();
	}
	
	private static void defineHsqlDatabase(Database database) throws Exception {
		Connection c = ((JDBCDatabase)database).getConnection();
		sql(c, DDL);
		c.commit();
	}
	
	private static void setup_hsql(boolean memory) throws Exception {
		Map<String, String> params = new HashMap<String, String>();
		params.put("dbStrictNameSpace", dbStrictNameSpace);
		params.put("session.jdbcDriver", session$jdbcDriver);
		params.put("session.jdbcUrl", memory ? session$jdbcUrl1 : session$jdbcUrl2);
		params.put("session.db", session$db);
		params.put("session.user", session$user);
		params.put("session.password", session$password);
		dbm = new SimpleDatabaseManager(dbName, dbClass, params);
		db = dbm.getDatabase();
		defineHsqlDatabase(db);
	}
	
	private static void setup_mysql(String parmFile) throws Exception {
		dbm = new SimpleDatabaseManager("file=" + parmFile);
		db = dbm.getDatabase();
	}
	
	private static void setup() throws Exception {
		if (db == null && !error) {
			if (MYSQL_PARM_FILE == null)
				setup_hsql(MEMORY_DB);
			else
				setup_mysql(MYSQL_PARM_FILE);
			setUpNumberType(db);
		}
	}
	
	public static Database getDatabase() throws Exception {
		try {
			setup();
		} catch (Exception e) {
			e.printStackTrace();
			error = true;
		}
		return db;
	}

	/**
	 * Just in case the DDL does not set up numerics.
	 * @param db
	 * @throws Exception
	 */
	private static void setUpNumberType(Database db)  throws Exception {
		try {
			db.getValueType("numeric").typeCheck(Double.class);
		} catch (Exception e) {
			UpdatableValueType<String> uvt = db.createValueType("numeric", false, "NUMBER");
			uvt.applyUpdates();
			@SuppressWarnings("rawtypes")
			UpdatableValueType<ValueType> uvtvt = db.getTypeBuiltInProperty().getValueType().typeCheck(ValueType.class).edit();
			uvtvt.addValue(uvtvt.getScanner().scan("numeric"), null);
			uvtvt.applyUpdates();
			db.commit();
		}
	}
	
	public static boolean inMemory() {
		return MYSQL_PARM_FILE == null && MEMORY_DB;
	}
	
	public static boolean canRollback() {
		return true;
	}

}
