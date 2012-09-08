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
 * Type: WriteMethodsForValueType
 * Version: 1.0.0
 */
package ch.agent.crnickl.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.api.ValueType;
import ch.agent.crnickl.impl.DatabaseBackend;
import ch.agent.crnickl.impl.Permission;
import ch.agent.crnickl.impl.ValueTypeImpl;
import ch.agent.crnickl.jdbc.T2DBJMsg.J;

/**
 * A stateless object with methods providing write access to value types.
 *  
 * @author Jean-Paul Vetterli
 * @version 1.0.0
 */
public class WriteMethodsForValueType extends JDBCDatabaseMethods {

	public WriteMethodsForValueType() {
	}
	
	private PreparedStatement create_valuetype;
	private static final String CREATE_VALUETYPE = 
		"insert into " + DB.VALUE_TYPE + " (label, restricted, builtin, scanner) " + 
		"values(?, ?, ?, ?)";
	/**
	 * Create a value type in the database.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param vt a value type
	 * @throws T2DBException
	 */
	public void createValueType(ValueType<?> vt) throws T2DBException {
		Surrogate surrogate = null;
		Throwable cause = null;
		try {
			check(Permission.CREATE, vt);
			create_valuetype = open(CREATE_VALUETYPE, vt, create_valuetype);
			create_valuetype.setString(1, vt.getName());
			create_valuetype.setBoolean(2, vt.isRestricted());
			create_valuetype.setBoolean(3, false);
			create_valuetype.setString(4, ((ValueTypeImpl<?>)vt).getExternalRepresentation());
			surrogate = makeSurrogate(vt, executeAndGetNewId(create_valuetype));
		} catch (Exception e) {
			cause = e;
		} finally {
			create_valuetype = close(create_valuetype);
		}
		if (surrogate == null || cause != null)
			throw T2DBJMsg.exception(cause, J.J10114, vt.getName());
		vt.getSurrogate().upgrade(surrogate);
	}

	private PreparedStatement delete_valuetype;
	private static final String DELETE_VALUETYPE = 
		"delete from " + DB.VALUE_TYPE + " where id = ?";
	/**
	 * Delete a value type from the database.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param vt a value type
	 * @throws T2DBException
	 */
	public void deleteValueType(ValueType<?> vt) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		try {
			check(Permission.MODIFY, vt);
			if (vt.isBuiltIn())
				throw T2DBJMsg.exception(J.J10120, vt.getName());
			int id = getId(vt);
			if (id <= DatabaseBackend.MAX_MAGIC_NR)
				throw T2DBJMsg.exception(J.J10120, vt.getName());
			int count = countProperties(vt);
			if (count > 0)
				throw T2DBJMsg.exception(J.J10119, vt.getName(), count);
			delete_valuetype = open(DELETE_VALUETYPE, vt, delete_valuetype);
			delete_valuetype.setInt(1, id);
			delete_valuetype.execute();
			done = delete_valuetype.getUpdateCount() > 0;
			if (done)
				deleteValues(vt);
		} catch (SQLException e) {
			cause = e;
		} finally {
			delete_valuetype = close(delete_valuetype);
		}
		if (!done || cause != null)
			throw T2DBJMsg.exception(cause, J.J10115, vt.getName());
	}
	
	private PreparedStatement update_valuetype;
	private static final String UPDATE_VALUETYPE = 
		"update " + DB.VALUE_TYPE + " set label = ? where id = ?";
	/**
	 * Update a value type in the database.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param vt a value type
	 * @throws T2DBException
	 */
	public void updateValueType(ValueType<?> vt) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		try {
			check(Permission.MODIFY, vt);
			update_valuetype = open(UPDATE_VALUETYPE, vt, update_valuetype);
			update_valuetype.setString(1, vt.getName());
			update_valuetype.setInt(2, getId(vt));
			update_valuetype.execute();
			done = update_valuetype.getUpdateCount() > 0;
		} catch (Exception e) {
			cause = e;
		} finally {
			update_valuetype = close(update_valuetype);
		}
		if (!done || cause != null)
			throw T2DBJMsg.exception(cause, J.J10116, vt.getName());
	}
	
	/**
	 * Add new values to the value type or update the description of existing values.
	 * The maps passed as parameter can be empty but cannot be null.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param vt a value type
	 * @param added a map of values to add and their descriptions
	 * @param edited a map of values to modify and their descriptions
	 * @param deleted a set of values to delete
	 * @throws T2DBException
	 */
	@SuppressWarnings("unchecked")
	public <T>void updateValueType(ValueType<T> vt, Map<T, String> added, Map<T, String> edited, Set<T> deleted) throws T2DBException {
		check(Permission.MODIFY, vt);
		for (Map.Entry<?, String> e : added.entrySet()) {
			// don't use vt.toString at this point, check will fail 
			insertValueTypeValue(vt, vt.getScanner().toString((T)e.getKey()), e.getValue());
		}
		for (Map.Entry<?, String> e : edited.entrySet()) {
			// don't use vt.toString at this point, check will fail 
			updateValueTypeValue(vt, vt.getScanner().toString((T)e.getKey()), e.getValue());
		}
		for (Object value : deleted) {
			deleteValueTypeValue(vt, vt.toString(value));
		}
	}
	
	private PreparedStatement insert_valuelist;
	private static final String INSERT_VALUELIST = 
		"insert into " + DB.VALUE_TYPE_VALUE + "(type, value, descrip) values(?, ?, ?)";
	/**
	 * Insert or update the value. Throw an exception if there is anything wrong.
	 * 
	 * @param session
	 * @param name
	 * @param id
	 * @param value
	 * @param description
	 * @throws T2DBException
	 */
	private <T>void insertValueTypeValue(ValueType<T> vt, String value, String description) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		if (value == null)
			throw new IllegalArgumentException("value null");
		if (description == null)
			description = "";
		try {
			insert_valuelist = open(INSERT_VALUELIST, vt, insert_valuelist);
			insert_valuelist.setInt(1, getId(vt));
			insert_valuelist.setString(2, value);
			insert_valuelist.setString(3, description);
			insert_valuelist.execute();
			done = insert_valuelist.getUpdateCount() > 0;
		} catch (Exception e) {
			cause = e;
		} finally {
			insert_valuelist = close(insert_valuelist);
		}
		if (!done || cause != null) {
			throw T2DBJMsg.exception(cause, J.J10121, vt.getName());
		}
	}
	
	private PreparedStatement update_valuelist;
	private static final String UPDATE_VALUELIST = 
		"update " + DB.VALUE_TYPE_VALUE + " set descrip = ? where type = ? and value = ?";
	private <T>void updateValueTypeValue(ValueType<T> vt, String value, String description) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		try {
			update_valuelist = open(UPDATE_VALUELIST, vt, update_valuelist);
			update_valuelist.setString(1, description);
			update_valuelist.setInt(2, getId(vt));
			update_valuelist.setString(3, value);
			update_valuelist.execute();
			done = update_valuelist.getUpdateCount() > 0;
		} catch (Exception e) {
			cause = e;
		} finally {
			update_valuelist = close(update_valuelist);
		}
		if (!done || cause != null) {
			throw T2DBJMsg.exception(cause, J.J10122, vt.getName());
		}
	}

	private PreparedStatement delete_valuelist;
	private static final String DELETE_VALUELIST = 
		"delete from " + DB.VALUE_TYPE_VALUE + " where type = ? and value = ?";
	/**
	 * Delete the value. Throw an exception if there is anything wrong.
	 * What can go wrong is that the value is in use, either as a 
	 * default value in an attribute definition or as an actual attribute value.
	 * 
	 * @param session
	 * @param name
	 * @param id
	 * @param value
	 * @throws T2DBException
	 */
	private <T>void deleteValueTypeValue(ValueType<T> vt, String value) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		String name = vt.getName();
		try {
			int id = getId(vt);
			int count = countDefaultValues(vt, value);
			if (count > 0)
				throw T2DBJMsg.exception(J.J10127, name, value, count);
			count = countActualValues(vt, value);
			if (count > 0)
				throw T2DBJMsg.exception(J.J10128, name, value, count);
			delete_valuelist = open(DELETE_VALUELIST, vt, delete_valuelist);
			delete_valuelist.setInt(1, id);
			delete_valuelist.setString(2, value);
			delete_valuelist.execute();
			done = delete_valuelist.getUpdateCount() > 0;
		} catch (SQLException e) {
			cause =e ;
		} finally {
			delete_valuelist = close(delete_valuelist);
		}
		if (!done|| cause != null)
			throw T2DBJMsg.exception(cause, J.J10126, name, value);
	}

	private PreparedStatement count_property;
	private static final String COUNT_PROPERTY = 
		"select count(*) from " + DB.PROPERTY + " where type = ?";
	private int countProperties(ValueType<?> vt) throws T2DBException,	SQLException {
		try {
			count_property = open(COUNT_PROPERTY, vt, count_property);
			count_property.setInt(1, getId(vt));
			ResultSet rs = count_property.executeQuery();
			rs.next();
			return rs.getInt(1);
		} finally {
			count_property = close(count_property);
		}
	}
	
	private PreparedStatement count_default_values;
	private static final String COUNT_DEFAULT_VALUES = 
		"select count(*) from " + DB.SCHEMA_ITEM + " s, " + DB.PROPERTY + " p " + 
		"where s.prop = p.id and p.type = ? and s.value = ?";
	private <T> int countDefaultValues(ValueType<T> vt, String value) throws T2DBException, SQLException {
		try {
			count_default_values = open(COUNT_DEFAULT_VALUES, vt, count_default_values);
			count_default_values.setInt(1, getId(vt));
			count_default_values.setString(2, value);
			ResultSet rs = count_default_values.executeQuery();
			rs.next();
			return rs.getInt(1);
		} finally {
			count_default_values = close(count_default_values);
		}
	}
	
	private PreparedStatement count_actual_values;
	private static final String COUNT_ACTUAL_VALUES = 
		"select count(*) from " + DB.PROPERTY + " p, " + DB.ATTRIBUTE_VALUE + " a " + 
		" where a.value = ? and p.type = ? and p.id = a.prop";
	private <T>int countActualValues(ValueType<T> vt, String value) throws T2DBException, SQLException {
		try {
			count_actual_values = open(COUNT_ACTUAL_VALUES, vt, count_actual_values);
			count_actual_values.setString(1, value);
			count_actual_values.setInt(2, getId(vt));
			ResultSet rs = count_actual_values.executeQuery();
			rs.next();
			return rs.getInt(1);
		} finally {
			count_actual_values = close(count_actual_values);
		}
	}
	
	private PreparedStatement delete_values;
	private static final String DELETE_VALUES = 
		"delete from " + DB.VALUE_TYPE_VALUE + " where type = ?";
	private void deleteValues(ValueType<?> vt) throws T2DBException, SQLException {
		try  {
			delete_values = open(DELETE_VALUES, vt, delete_values);
			delete_values.setInt(1, getId(vt));
			delete_values.execute();
		} finally {
			delete_values = close(delete_values);
		}
	}

}