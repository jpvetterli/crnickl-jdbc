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

import java.sql.PreparedStatement;
import java.sql.SQLException;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg;
import ch.agent.crnickl.T2DBMsg.E;
import ch.agent.crnickl.api.Property;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.api.ValueType;
import ch.agent.crnickl.impl.DatabaseBackend;
import ch.agent.crnickl.impl.Permission;
import ch.agent.crnickl.impl.SchemaUpdatePolicy;

/**
 * A stateless object with methods providing write access to properties.
 *  
 * @author Jean-Paul Vetterli
 */
public class WriteMethodsForProperty extends ReadMethodsForProperty {

	public WriteMethodsForProperty() {
	}
	
	private PreparedStatement create_property;
	private static final String CREATE_PROPERTY = 
		"insert into " + DB.PROPERTY + "(type, label) values(?, ?)";
	/**
	 * Create a new property and return its key. 
	 * If creation fails throw an exception.
	 * 
	 * @param property a property
	 * @throws T2DBException
	 */
	public void createProperty(Property<?> property) throws T2DBException {
		Surrogate surrogate = null;
		Throwable cause = null;
		try {
			check(Permission.CREATE, property);
			create_property = open(CREATE_PROPERTY, property, create_property);
			ValueType<?> vt = property.getValueType();
			create_property.setInt(1, getId(vt));
			create_property.setString(2, property.getName());
			surrogate = makeSurrogate(property, executeAndGetNewId(create_property));
		} catch (Exception e) {
			cause = e;
		} finally {
			create_property = close(create_property);
		}
		if (surrogate == null || cause != null)
			throw T2DBMsg.exception(cause, E.E20114, property.getName());
		property.getSurrogate().upgrade(surrogate);
	}

	private PreparedStatement delete_property;
	private static final String DELETE_PROPERTY = 
		"delete from " + DB.PROPERTY + " where id = ?";
	/**
	 * Delete the property.
	 * If deleting fails throw an exception.
	 * 
	 * @param property a property
	 * @param policy a schema updating policy
	 * @throws T2DBException
	 */
	public void deleteProperty(Property<?> property, SchemaUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		try {
			check(Permission.MODIFY, property);
			int id = getId(property);
			if (id <= DatabaseBackend.MAX_MAGIC_NR)
				throw T2DBMsg.exception(E.E20120, property.getName());
			policy.willDelete(property);
			delete_property = open(DELETE_PROPERTY, property, delete_property);
			delete_property.setInt(1, id);
			delete_property.execute();
			done = delete_property.getUpdateCount() > 0;
		} catch (SQLException e) {
			cause = e;
		} finally {
			delete_property = close(delete_property);
		}
		if (!done || cause != null)
			throw T2DBMsg.exception(cause, E.E20115, property.getName());
	}
	
	private PreparedStatement update_property;
	private static final String UPDATE_PROPERTY = 
		"update " + DB.PROPERTY + " set label = ? where id = ?";
	/**
	 * Update the name of the property.
	 * If updating fails throw an exception.
	 * 
	 * @param property a property
	 * @param policy a schema updating policy
	 * @throws T2DBException
	 */
	public void updateProperty(Property<?> property, SchemaUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		try {
			check(Permission.MODIFY, property);
			update_property = open(UPDATE_PROPERTY, property, update_property);
			update_property.setString(1, property.getName());
			update_property.setInt(2, getId(property));
			update_property.execute();
			done = update_property.getUpdateCount() > 0;
		} catch (Exception e) {
			cause = e;
		} finally {
			update_property = close(update_property);
		}
		if (!done || cause != null)
			throw T2DBMsg.exception(cause, E.E20116, property.getName());
	}
	
}
