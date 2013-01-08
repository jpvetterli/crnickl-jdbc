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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg;
import ch.agent.crnickl.T2DBMsg.E;
import ch.agent.crnickl.api.DBObjectType;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Property;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.api.ValueType;
import ch.agent.crnickl.impl.PropertyImpl;
import ch.agent.crnickl.jdbc.T2DBJMsg.J;

/**
 * A stateless object with methods providing read access to properties.
 *  
 * @author Jean-Paul Vetterli
 */
public class ReadMethodsForProperty extends JDBCDatabaseMethods {

	public ReadMethodsForProperty() {
	}

	private PreparedStatement select_property_by_name;
	private static final String SELECT_PROPERTY_BY_NAME = 
		"select id, type, label from " + DB.PROPERTY + " where label = ?";
	/**
	 * Find a property by its name in a database.
	 * 
	 * @param database a database 
	 * @param name a string 
	 * @return a property or null
	 * @throws T2DBException
	 */
	public Property<?> getProperty(Database database, String name) throws T2DBException {
		try {
			select_property_by_name = open(SELECT_PROPERTY_BY_NAME, database, select_property_by_name);
			select_property_by_name.setString(1, name);
			ResultSet rs = select_property_by_name.executeQuery();
			if (rs.next()) {
				Surrogate surrogate = makeSurrogate(database, DBObjectType.PROPERTY, rs.getInt(1));
				return getProperty(surrogate, rs.getString(3), rs.getInt(2));
			} else
				return null;
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E20104, name);
		} finally {
			select_property_by_name = close(select_property_by_name);
		}
	}
	
	private PreparedStatement select_property_by_pattern;
	private static final String SELECT_PROPERTY_BY_PATTERN = 
		"select id, type, label from " + DB.PROPERTY + " where label like ? order by label";
	/**
	 * Find a collection of properties with names matching a simple pattern.
	 * @param database a database
	 * @param pattern a simple pattern where "*" stands for zero or more characters
	 * @return a collection of properties, possibly empty, never null
	 * @throws T2DBException
	 */
	public Collection<Property<?>> getProperties(Database database, String pattern) throws T2DBException {
		if (pattern == null)
			pattern = "*";
		pattern = pattern.replace('*', '%');
		Collection<Property<?>> result = new ArrayList<Property<?>>();
		try {
			select_property_by_pattern = open(SELECT_PROPERTY_BY_PATTERN, database, select_property_by_pattern);
			select_property_by_pattern.setString(1, pattern);
			ResultSet rs = select_property_by_pattern.executeQuery();
			while(rs.next()) {
				Surrogate surrogate = makeSurrogate(database, DBObjectType.PROPERTY, rs.getInt(1));
				result.add(getProperty(surrogate, rs.getString(3), rs.getInt(2)));
			}
			return result;
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E20106, pattern);
		} finally {
			select_property_by_pattern = close(select_property_by_pattern);
		}
	}
	
	private PreparedStatement select_property_by_id;
	private static final String SELECT_PROPERTY_BY_ID = 
		"select id, type, label from " + DB.PROPERTY + " where id = ?";
	/**
	 * Find a property corresponding to a surrogate.
	 * 
	 * @param surrogate a surrogate
	 * @return a property or null
	 * @throws T2DBException
	 */
	public Property<?> getProperty(Surrogate surrogate) throws T2DBException {
		try {
			select_property_by_id = open(SELECT_PROPERTY_BY_ID, surrogate, select_property_by_id);
			select_property_by_id.setInt(1, getId(surrogate));
			ResultSet rs = select_property_by_id.executeQuery();
			if (rs.next())
				return getProperty(surrogate, rs.getString(3), rs.getInt(2));
			else
				return null;
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E20105, surrogate.toString());
		} finally {
			select_property_by_id = close(select_property_by_id);
		}
	}
	
	private ReadMethodsForValueType getVTRMethods(Surrogate surrogate) throws T2DBException {
		try {
			return ((JDBCDatabase) surrogate.getDatabase()).getReadMethodsForValueType();
		} catch (Exception e) {
			throw T2DBJMsg.exception(J.J01101, surrogate.getDatabase().getClass().getName(), JDBCDatabase.class.getName());
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <T>Property<?> getProperty(Surrogate surrogate, String name, int valueTypeId) throws T2DBException, SQLException {
		Surrogate vtKey = makeSurrogate(surrogate.getDatabase(), DBObjectType.VALUE_TYPE, valueTypeId);
		ValueType<?> vt = getVTRMethods(surrogate).getValueType(vtKey);
		return new PropertyImpl(name, vt, true, surrogate);
	}
	
}
