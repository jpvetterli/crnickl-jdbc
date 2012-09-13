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
import java.sql.Statement;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.api.DBObject;
import ch.agent.crnickl.api.DBObjectId;
import ch.agent.crnickl.api.DBObjectType;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.impl.DatabaseMethodsImpl;
import ch.agent.crnickl.jdbc.T2DBJMsg.J;

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
	 * Return the internal ID of a database object or 0 if the object is null or 
	 * is <em>in construction</em>.
	 * The internal ID is not exposed to clients.
	 * <p>
	 * This method is for use inside the JDBC implementation
	 * and its argument must have a {@link DBObjectId} implemented
	 * by {@link JDBCObjectId}.
	 * 
	 * @param dBObject a database object or null
	 * @return a non negative integer
	 */
	public int getIdOrZero(DBObject dBObject) {
		try {
			if (dBObject != null && !dBObject.inConstruction())
				return ((JDBCObjectId) dBObject.getId()).value();
			else
				return 0;
		} catch(ClassCastException e) {
			throw new RuntimeException("bug: " + dBObject.toString(), e);
		}
	}
	
	/**
	 * Return the internal ID of a database object.
	 * The internal ID is not exposed to clients.
	 * <p>
	 * This method is for use inside the JDBC implementation
	 * and its argument must have a {@link DBObjectId} implemented
	 * by {@link JDBCObjectId}.
	 * 
	 * @param dBObject a database object
	 * @return a positive integer
	 */
	protected int getId(DBObject dBObject) {
		int id = getIdOrZero(dBObject);
		if (id < 1)
			throw new RuntimeException("bug (database integrity violation)");
		return id;
	}

	/**
	 * Extract the internal ID of a database object from its surrogate.
	 * The internal ID is not exposed to clients.
	 * <p>
	 * This method is for use inside the JDBC implementation
	 * and its argument must have a {@link DBObjectId} implemented
	 * by {@link JDBCObjectId}.
	 * 
	 * @param surrogate the surrogate of a database object
	 * @return a positive integer
	 */
	public int getId(Surrogate surrogate) {
		try {
			int id = ((JDBCObjectId) surrogate.getId()).value();
			if (id < 1)
				throw new RuntimeException("bug (database integrity violation)");
			return id;
		} catch(ClassCastException e) {
			throw new RuntimeException("bug: " + surrogate.toString(), e);
		}

	}
	
	/**
	 * Create a surrogate for a database object.
	 * 
	 * @param db the database of the object
	 * @param dot the type of the object
	 * @param id the internal ID of the database object
	 * @return a surrogate
	 */
	public Surrogate makeSurrogate(Database db, DBObjectType dot, int id) {
		return super.makeSurrogate(db,  dot, new JDBCObjectId(id));
	}
	
	/**
	 * Execute a prepared statement of the "insertion" type and return the 
	 * generated id.
	 * 
	 * @param stmt a prepared statement
	 * @return a generated id
	 * @throws T2DBException
	 * @throws SQLException
	 */
	public DBObjectId executeAndGetNewId(PreparedStatement stmt) throws T2DBException, SQLException {
		int id = -1;
		stmt.execute();
		if (stmt.getUpdateCount() > 0) {
			ResultSet rs = stmt.getGeneratedKeys();
			if (rs.next())
				id = rs.getInt(1);
		}
		if (id < 0)
			throw T2DBJMsg.exception(J.J00108);
		return new JDBCObjectId(id);
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
