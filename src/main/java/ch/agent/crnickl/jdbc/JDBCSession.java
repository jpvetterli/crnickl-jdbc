/*
 *   Copyright 2012-2013 Hauser Olsson GmbH
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
 */
package ch.agent.crnickl.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg;
import ch.agent.crnickl.T2DBMsg.E;
import ch.agent.crnickl.api.DatabaseConfiguration;
import ch.agent.crnickl.jdbc.T2DBJMsg.J;

/**
 * JDBCSession is a singleton keeping track of the JDBC connection.
 * 
 * @author Jean-Paul Vetterli
 */
public class JDBCSession {

	private static class Singleton {
		private static JDBCSession jdbc_connection;
		static {
			jdbc_connection = new JDBCSession();
		};
	}

	private DatabaseConfiguration configuration;
	private String url;
	private String user;

	private Connection connection = null;
	
	public static final String JDBC_DRIVER = "session.jdbcDriver";
	public static final String JDBC_URL = "session.jdbcUrl"; // must contain a %s for session.db
	public static final String JDBC_DB = "session.db";
	public static final String JDBC_USER = "session.user";
	public static final String JDBC_PASSWORD = "session.password";

	private JDBCSession() {
	}

	/**
	 * Construct a JDBC session. This constructor can be invoked only once.
	 * 
	 * @param configuration a database configuration
	 */
	public JDBCSession(DatabaseConfiguration configuration) {
		if (Singleton.jdbc_connection.configuration != null)
			throw new IllegalStateException("already initialized");
		Singleton.jdbc_connection.configuration = configuration;
	}
	
	/**
	 * Return the JDBC session.
	 * 
	 * @return the JDBC session
	 */
	public static JDBCSession getInstance() {
		if (Singleton.jdbc_connection.configuration == null)
			throw new IllegalStateException("not initialized");
		return Singleton.jdbc_connection;
	}
	
	/**
	 * Perform a rollback if there is a session, else do nothing.
	 * Catch and discard any exception.
	 */
	public static void rollbackIfAlive() {
		try {
			if (Singleton.jdbc_connection.configuration != null)
				Singleton.jdbc_connection.rollback();
		} catch (T2DBException e) {
			// ignore
		}
	}
	
	private void open() throws T2DBException {
		Properties prop = new Properties();
		String driver = null;
		try {
			driver = configuration.getParameter(JDBC_DRIVER, true);
			Class.forName(driver);
			String db = configuration.getParameter(JDBC_DB, true);
			url = String.format(configuration.getParameter(JDBC_URL, true), db);
			user = configuration.getParameter(JDBC_USER, true);
			prop.setProperty("user", user);
			prop.setProperty("password", configuration.getParameter(JDBC_PASSWORD, true));
			// don't zap credentials so we can restart
			// next one is a workaround for a mysql problem
			prop.setProperty("useServerPrepStmts", "false");
			connection = DriverManager.getConnection(url, prop);
			connection.setAutoCommit(false);
		} catch (ClassNotFoundException e) {
			throw T2DBJMsg.exception(e, J.J00105);
		} catch (SQLException e) {
			throw T2DBJMsg.exception(e, J.J00104, driver, toString());
		} finally {
			prop.setProperty("user", "xxx");
			prop.setProperty("password", "xxx");
		}
	}
	
	/**
	 * Close the JDBC connection if it is open.
	 */
	public void close(boolean ignoreException) throws T2DBException {
		try {
			if (isOpen())
				connection.close();
			connection = null;
			configuration = null;
			user = null;
			url = null;
		} catch (Exception e) {
			if (!ignoreException)
				throw T2DBMsg.exception(E.E00110, toString());
		}
	}

	/**
	 * Return the JDBC connection. If there is no connection, open one.
	 * 
	 * @return a JDBC connection
	 */
	public Connection getConnection() throws T2DBException {
		if (!isOpen())
			open();
		return connection;
	}
	
	private boolean isOpen() {
		return connection != null;
	}

	/**
	 * Commit the current transaction.
	 * 
	 * @throws T2DBException
	 */
	public void commit() throws T2DBException {
		try {
			getConnection().commit();
		} catch (SQLException e) {
			throw T2DBJMsg.exception(e, J.J00106);
		}
	}
	
	/**
	 * Rollback the current transaction.
	 * 
	 * @throws T2DBException
	 */
	public void rollback() throws T2DBException {
		try {
			getConnection().rollback();
		} catch (SQLException e) {
			throw T2DBJMsg.exception(e, J.J00107);
		}
	}
	
	/**
	 * Return the URL of the session.
	 * 
	 * @return the URL
	 */
	public String getURL() {
		return url;
	}
	
	/**
	 * Return the user id of the session.
	 * 
	 * @return the user id
	 */
	public String getUser() {
		return user;
	}

	/**
	 * Return a string displaying the session with the URL and the user id.
	 * 
	 * @return a string displaying the session
	 */
	@Override
	public String toString() {
		return String.format("%s@%s", user, url);
	}
	
	

}
