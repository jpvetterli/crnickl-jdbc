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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg.D;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.api.ValueType;
import ch.agent.crnickl.impl.Permission;
import ch.agent.crnickl.impl.SchemaUpdatePolicy;
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
		"insert into " + DB.VALUE_TYPE + " (label, restricted, scanner) " + 
		"values(?, ?, ?)";
	/**
	 * Create a value type in the database.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param vt a value type
	 * @throws T2DBException
	 */
	public <T>void createValueType(ValueType<T> vt) throws T2DBException {
		Surrogate surrogate = null;
		Throwable cause = null;
		try {
			check(Permission.CREATE, vt);
			create_valuetype = open(CREATE_VALUETYPE, vt, create_valuetype);
			create_valuetype.setString(1, vt.getName());
			create_valuetype.setBoolean(2, vt.isRestricted());
			create_valuetype.setString(3, ((ValueTypeImpl<T>)vt).getExternalRepresentation());
			surrogate = makeSurrogate(vt, executeAndGetNewId(create_valuetype));
			vt.getSurrogate().upgrade(surrogate);
			updateValues(null, vt, null);
		} catch (Exception e) {
			cause = e;
		} finally {
			create_valuetype = close(create_valuetype);
		}
		if (surrogate == null || cause != null)
			throw T2DBJMsg.exception(cause, J.J10114, vt.getName());
	}

	private PreparedStatement delete_valuetype;
	private static final String DELETE_VALUETYPE = 
		"delete from " + DB.VALUE_TYPE + " where id = ?";
	/**
	 * Delete a value type from the database.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param vt a value type
	 * @param policy a schema udpdating policy
	 * @throws T2DBException
	 */
	public <T>void deleteValueType(ValueType<T> vt, SchemaUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		try {
			check(Permission.MODIFY, vt);
			policy.willDelete(vt);
			if (vt.isRestricted())
				deleteValues(vt);
			delete_valuetype = open(DELETE_VALUETYPE, vt, delete_valuetype);
			delete_valuetype.setInt(1, getId(vt));
			delete_valuetype.execute();
			done = delete_valuetype.getUpdateCount() > 0;
		} catch (Exception e) {
			cause = e;
		} finally {
			delete_valuetype = close(delete_valuetype);
		}
		if (!done || cause != null)
			throw T2DBJMsg.exception(cause, D.D10145, vt.getName());
	}
	
	private PreparedStatement update_valuetype;
	private static final String UPDATE_VALUETYPE = 
		"update " + DB.VALUE_TYPE + " set label = ? where id = ?";
	/**
	 * Update a value type in the database.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param vt a value type
	 * @param policy a schema udpdating policy
	 * @throws T2DBException
	 */
	public <T>void updateValueType(ValueType<T> vt, SchemaUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		try {
			check(Permission.MODIFY, vt);
			Surrogate s = vt.getSurrogate();
			ValueType<T> original = 
					((JDBCDatabase) s.getDatabase()).getReadMethodsForValueType().getValueType(s);
			if (!original.getName().equals(vt.getName())) {
				update_valuetype = open(UPDATE_VALUETYPE, vt, update_valuetype);
				update_valuetype.setString(1, vt.getName());
				update_valuetype.setInt(2, getId(vt));
				update_valuetype.execute();
				done = update_valuetype.getUpdateCount() > 0;
			}
			done |= updateValues(original, vt, policy);
		} catch (Exception e) {
			cause = e;
		} finally {
			update_valuetype = close(update_valuetype);
		}
		if (!done || cause != null)
			throw T2DBJMsg.exception(cause, D.D10146, vt.getName());
	}
	
	private <T>boolean updateValues(ValueType<T> original, ValueType<T> vt, SchemaUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		if (vt.isRestricted()) {
			Map<T, String> added;
			Set<T> deleted;
			Map<T, String> edited;
			Map<T, String> updates = vt.getValueDescriptions();
			if (original == null) {
				added = updates;
				edited = new HashMap<T, String>();
				deleted = new HashSet<T>();
			} else {
				Map<T, String> current = (Map<T, String>) original.getValueDescriptions();
				Set<T> addedKeys = new HashSet<T>(updates.keySet());
				addedKeys.removeAll(current.keySet());
				added = new HashMap<T, String>();
				for (T key : addedKeys) {
					added.put(key, updates.get(key));
				}
				
				deleted = new HashSet<T>(current.keySet());
				deleted.removeAll(updates.keySet());
				
				edited = updates;
				for (T key : addedKeys) {
					edited.remove(key);
				}
			}
			done = updateValueType(vt, added, edited, deleted, policy);
		}
		return done;
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
	 * @param policy update policy
	 * @return true if anything done
	 * @throws T2DBException
	 */
	private <T>boolean updateValueType(ValueType<T> vt, Map<T, String> added, Map<T, String> edited, Set<T> deleted, SchemaUpdatePolicy policy) throws T2DBException {
		int count = 0;
		for (Map.Entry<T, String> e : added.entrySet()) {
			// don't use vt.toString at this point, check will fail 
			insertValueTypeValue(vt, vt.getScanner().toString(e.getKey()), e.getValue());
			count++;
		}
		for (Map.Entry<T, String> e : edited.entrySet()) {
			// don't use vt.toString at this point, check will fail 
			updateValueTypeValue(vt, vt.getScanner().toString(e.getKey()), e.getValue());
			count++;
		}
		for (T value : deleted) {
			deleteValueTypeValue(vt, value, policy);
			count++;
		}
		return count > 0;
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
	 * @param policy update policy or null (when creating)
	 * @throws T2DBException
	 */
	private <T>void deleteValueTypeValue(ValueType<T> vt, T value, SchemaUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		String name = vt.getName();
		try {
			int id = getId(vt);
			if (policy != null)	
				policy.willDelete(vt, value);
			delete_valuelist = open(DELETE_VALUELIST, vt, delete_valuelist);
			delete_valuelist.setInt(1, id);
			delete_valuelist.setString(2, vt.toString(value));
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
