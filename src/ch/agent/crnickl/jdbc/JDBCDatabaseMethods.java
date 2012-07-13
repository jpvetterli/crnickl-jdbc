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
 * Package: ch.agent.crnickl.jdbc
 * Type: JDBCDatabaseMethods
 * Version: 1.0.0
 */
package ch.agent.crnickl.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.api.DBObject;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.impl.DatabaseMethodsImpl;
import ch.agent.crnickl.jdbc.T2DBJMsg.J;

import com.mysql.jdbc.Statement;

/**
 * A JDBCDatabaseMethods object provides support for prepared statements. It is
 * meant as base class for actual access methods. All database accesses should
 * be bracketed between one of the <code>open</code> and the <code>close</code>
 * method.
 * 
 * @author Jean-Paul Vetterli
 * @version 1.0.0
 */
public class JDBCDatabaseMethods extends DatabaseMethodsImpl {

	/**
	 * Execute a prepared statement of the "insertion" type and return the 
	 * generated key.
	 * 
	 * @param stmt a prepared statement
	 * @return a generated key
	 * @throws T2DBException
	 * @throws SQLException
	 */
	public int executeAndGetNewId(PreparedStatement stmt) throws T2DBException, SQLException {
		int id = -1;
		stmt.execute();
		if (stmt.getUpdateCount() > 0) {
			ResultSet rs = stmt.getGeneratedKeys();
			if (rs.next())
				id = rs.getInt(1);
		}
		if (id < 0)
			throw T2DBJMsg.exception(J.J00108);
		return id;
	}
	
	/**
	 * Return a valid prepared statement for the SQL code specified. The
	 * statement is prepared using the connection embedded in the database
	 * parameter. To avoid preparing a new statement again and again a statement
	 * can be passed as an argument. This statement will be returned if it is
	 * not null and if its connection is equal to the connection embedded in the
	 * mentioned parameter.
	 * 
	 * @param sql
	 *            a string in SQL syntax
	 * @param database
	 *            a database
	 * @param stmt
	 *            a prepared statement
	 * @return a prepared statement
	 * @throws T2DBException
	 */
	public PreparedStatement open(String sql, Database database, PreparedStatement stmt) throws T2DBException {
		try {
			if (stmt != null && ((JDBCDatabase) database).getConnection().equals(stmt.getConnection()))
				return stmt;
			else 
				return ((JDBCDatabase) database).getConnection().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		} catch (SQLException e) {
			throw T2DBJMsg.exception(e, J.J00101, sql);
		}
	}

	/**
	 * Return a valid prepared statement for the SQL code specified. The
	 * statement is prepared using the connection embedded in the
	 * <code>dBObject</code> parameter. To avoid preparing a new statement again
	 * and again a statement can be passed as an argument. This statement will
	 * be returned if it is not null and if its connection is equal to
	 * connection embedded in the mentioned parameter.
	 * 
	 * @param sql
	 *            a string in SQL syntax
	 * @param dBObject
	 *            a database object
	 * @param stmt
	 *            a prepared statement
	 * @return a prepared statement
	 * @throws T2DBException
	 */
	public PreparedStatement open(String sql, DBObject dBObject,
			PreparedStatement stmt) throws T2DBException {
		return open(sql, dBObject.getSurrogate().getDatabase(), stmt);
	}
	
	/**
	 * Return a valid prepared statement for the SQL code specified. The
	 * statement is prepared using the connection embedded in the
	 * <code>surrogate</code> parameter. To avoid preparing a new statement again
	 * and again a statement can be passed as an argument. This statement will
	 * be returned if it is not null and if its connection is equal to
	 * connection embedded in the mentioned parameter.
	 * 
	 * @param sql
	 *            a string in SQL syntax
	 * @param surrogate
	 *            a surrogate
	 * @param stmt
	 *            a prepared statement
	 * @return a prepared statement
	 * @throws T2DBException
	 */
	public PreparedStatement open(String sql, Surrogate surrogate, PreparedStatement stmt) throws T2DBException {
		return open(sql, surrogate.getDatabase(), stmt);
	}
	
	/**
	 * Close the prepared statement. This implementation actually closes the
	 * statement and returns null. Possibly, a future implementation could
	 * decide not to close the statement and to return it to the client. For
	 * this reason it is meaningful for clients to cache the statement for
	 * reuse, with a possible positive effect on performance.
	 * 
	 * @param stmt
	 *            a prepared statement
	 * @return the prepared statement, set to null
	 * @throws T2DBException
	 */
	public PreparedStatement close(PreparedStatement stmt) throws T2DBException {
		try {
			if (stmt != null) {
				stmt.close();
				stmt = null;
			}
			return stmt;
		} catch (SQLException e) {
			throw T2DBJMsg.exception(e, J.J00102);
		}
	}
	
	
}
