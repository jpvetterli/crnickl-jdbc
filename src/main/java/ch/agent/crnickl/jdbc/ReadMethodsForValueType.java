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
 * Type: ReadMethodsForValueType
 * Version: 1.0.0
 */
package ch.agent.crnickl.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.api.DBObjectType;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.api.ValueType;
import ch.agent.crnickl.impl.ValueTypeImpl;
import ch.agent.crnickl.jdbc.T2DBJMsg.J;

/**
 * A stateless object with methods providing read access to value types.
 *  
 * @author Jean-Paul Vetterli
 * @version 1.0.0
 */
public class ReadMethodsForValueType extends JDBCDatabaseMethods {

	public ReadMethodsForValueType() {
	}

	private PreparedStatement select_valuetype_by_name;
	private static final String SELECT_VALUETYPE_BY_NAME = 
		"select id, restricted, scanner, lastmod "
		+ "from " + DB.VALUE_TYPE + " where label = ?";
	/**
	 * Find a value type with a given name.
	 * 
	 * @param database a database
	 * @param name a string
	 * @return a value type or null
	 * @throws T2DBException
	 */
	public <T>ValueType<T> getValueType(Database database, String name) throws T2DBException {
		try {
			select_valuetype_by_name = open(SELECT_VALUETYPE_BY_NAME, database, select_valuetype_by_name);
			select_valuetype_by_name.setString(1, name);
			ResultSet rs = select_valuetype_by_name.executeQuery();
			if (rs.next()) {
				Surrogate surrogate = makeSurrogate(database, DBObjectType.VALUE_TYPE, rs.getInt(1));
				return getValueType(surrogate, name, rs.getBoolean(2), rs.getString(3));
			} else
				return null;
		} catch (Exception e) {
			throw T2DBJMsg.exception(e, J.J10104, name);
		} finally {
			select_valuetype_by_name = close(select_valuetype_by_name);
		}
	}
	
	private PreparedStatement select_valuetype_by_pattern;
	private static final String SELECT_VALUETYPE_BY_PATTERN = 
		"select id, label, restricted, scanner, lastmod "
		+ "from " + DB.VALUE_TYPE + " where label like ? order by label";
	/**
	 * Find a collection of value types with names matching a pattern.
	 * 
	 * @param database a database
	 * @param pattern a simple pattern where "*" stands for zero or more characters
	 * @return a collection of value types, possibly empty, never null
	 * @throws T2DBException
	 */
	public Collection<ValueType<?>> getValueTypes(Database database, String pattern) throws T2DBException {
		if (pattern == null)
			pattern = "*";
		pattern = pattern.replace('*', '%');
		Collection<ValueType<?>> result = new ArrayList<ValueType<?>>();
		try {
			select_valuetype_by_pattern = open(SELECT_VALUETYPE_BY_PATTERN, database, select_valuetype_by_pattern);
			select_valuetype_by_pattern.setString(1, pattern);
			ResultSet rs = select_valuetype_by_pattern.executeQuery();
			while(rs.next()) {
				Surrogate surrogate = makeSurrogate(database, DBObjectType.VALUE_TYPE, rs.getInt(1));
				result.add(getValueType(surrogate, rs.getString(2), rs.getBoolean(3), rs.getString(4)));
			}
			return result;
		} catch (Exception e) {
			throw T2DBJMsg.exception(e, J.J10106, pattern);
		} finally {
			select_valuetype_by_pattern = close(select_valuetype_by_pattern);
		}
	}
	
	private PreparedStatement select_valuetype_by_id;
	private static final String SELECT_VALUETYPE_BY_ID = 
		"select label, restricted, scanner, lastmod "
		+ "from " + DB.VALUE_TYPE + " where id = ?";
	/**
	 * Find a value type corresponding to a surrogate.
	 * 
	 * @param surrogate a surrogate
	 * @return a value type or null
	 * @throws T2DBException
	 */
	public <T>ValueType<T> getValueType(Surrogate surrogate) throws T2DBException {
		try {
			select_valuetype_by_id = open(SELECT_VALUETYPE_BY_ID, surrogate, select_valuetype_by_id);
			select_valuetype_by_id.setInt(1, getId(surrogate));
			ResultSet rs = select_valuetype_by_id.executeQuery();
			if (rs.next())
				return getValueType(surrogate, rs.getString(1), rs.getBoolean(2), rs.getString(3));
			else
				return null;
		} catch (Exception e) {
			throw T2DBJMsg.exception(e, J.J10105, surrogate.toString());
		} finally {
			select_valuetype_by_id = close(select_valuetype_by_id);
		}
	}
	
	private PreparedStatement select_valuelist_by_id;
	private static final String SELECT_VALUELIST_BY_ID = 
		"select value, descrip from " + DB.VALUE_TYPE_VALUE + " where type = ? order by value";
	private Map<String, String> getValues(Surrogate surrogate) throws T2DBException, SQLException {
		Map<String, String> values = new LinkedHashMap<String, String>();
		try {
			select_valuelist_by_id = open(SELECT_VALUELIST_BY_ID, surrogate, select_valuelist_by_id);
			select_valuelist_by_id.setInt(1, getId(surrogate));
			ResultSet rs = select_valuelist_by_id.executeQuery();
			while (rs.next()) {
				values.put(rs.getString(1), rs.getString(2));
			}
		} finally {
			select_valuelist_by_id = close(select_valuelist_by_id);
		}
		return values;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <T>ValueType<T> getValueType(Surrogate surrogate, String name, boolean restricted, String scannerClassOrKeyword) throws T2DBException, SQLException {
		Map<String, String> values = null;
		if (restricted)
			values = getValues(surrogate);
		return new ValueTypeImpl(name, restricted, scannerClassOrKeyword, values, surrogate);
	}
	
}
