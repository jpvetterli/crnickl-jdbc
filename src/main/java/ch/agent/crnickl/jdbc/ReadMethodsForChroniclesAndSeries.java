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
 * Type: ReadMethodsForChroniclesAndSeries
 * Version: 1.0.0
 */
package ch.agent.crnickl.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg;
import ch.agent.crnickl.T2DBMsg.E;
import ch.agent.crnickl.api.Attribute;
import ch.agent.crnickl.api.Chronicle;
import ch.agent.crnickl.api.DBObject;
import ch.agent.crnickl.api.DBObjectType;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Property;
import ch.agent.crnickl.api.Series;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.impl.ChronicleImpl;
import ch.agent.crnickl.impl.ChronicleImpl.RawData;
import ch.agent.crnickl.impl.Permission;
import ch.agent.crnickl.impl.SeriesImpl;

/**
 * A stateless object with methods providing read access to chronicles and
 * series.
 * 
 * @author Jean-Paul Vetterli
 * @version 1.0.0
 */
public class ReadMethodsForChroniclesAndSeries extends JDBCDatabaseMethods {
	
	private static int MAX_ENTITY_DEPTH = 6;
	
	public ReadMethodsForChroniclesAndSeries() {
	}

	private PreparedStatement select_entity_by_id;
	private static final String SELECT_ENTITY_BY_ID = 
		"select parent, name, schema_id, descrip from " + DB.CHRONICLE + " where id = ?";
	/**
	 * Find a chronicle corresponding to a surrogate. An
	 * exception is thrown if there is no such chronicle.
	 * 
	 * @param surrogate a surrogate
	 * @return a chronicle, never null
	 * @throws T2DBException
	 */
	public Chronicle getChronicle(Surrogate surrogate) throws T2DBException {
		Chronicle chronicle = null;
		Throwable cause = null;
		try {
			select_entity_by_id = open(SELECT_ENTITY_BY_ID, surrogate, select_entity_by_id);

			select_entity_by_id.setInt(1, getId(surrogate));
			ResultSet rs = select_entity_by_id.executeQuery();
			if (rs.next()) {
				ChronicleImpl.RawData data = new ChronicleImpl.RawData();
				data.setSurrogate(surrogate);
				data.setName(rs.getString(2));
				data.setDescription(rs.getString(4));
				int collection = rs.getInt(1);
				data.setCollection(collection == 0 ? surrogate.getDatabase().getTopChronicle() : 
					new ChronicleImpl(makeSurrogate(surrogate.getDatabase(), DBObjectType.CHRONICLE, collection)));
				int schema = rs.getInt(3);
				data.setSchema(schema == 0 ? null : makeSurrogate(surrogate.getDatabase(), DBObjectType.SCHEMA, schema));
				chronicle = new ChronicleImpl(data);
				check(Permission.READ, chronicle);
			}
			rs.close();
		} catch (SQLException e) {
			cause = e;
		} finally {
			select_entity_by_id = close(select_entity_by_id);
		}
		if (chronicle == null || cause != null)
			throw T2DBMsg.exception(cause, E.E40104, surrogate.toString());
		return chronicle;
	}
	
	private PreparedStatement select_entity_by_parent_and_name;
	private static final String SELECT_ENTITY_BY_PARENT_AND_NAME = 
		"select id, schema_id, descrip from " + DB.CHRONICLE + " where parent = ? and name = ?";
	/**
	 * Find a chronicle with a given parent and name.
	 * 
	 * @param parent a chronicle
	 * @param name a string 
	 * @return a chronicle or null
	 * @throws T2DBException
	 */
	public Chronicle getChronicleOrNull(Chronicle parent, String name) throws T2DBException {
		Chronicle chronicle = null;
		try {
			select_entity_by_parent_and_name = open(SELECT_ENTITY_BY_PARENT_AND_NAME, parent, select_entity_by_parent_and_name);
			select_entity_by_parent_and_name.setInt(1, getIdOrZero(parent));
			select_entity_by_parent_and_name.setString(2, name);
			ResultSet rs = select_entity_by_parent_and_name.executeQuery();
			if (rs.next()) {
				int id = rs.getInt(1);
				if (id == 0)
					throw T2DBMsg.exception(E.E40105, parent == null ? null : parent.toString(), name);
				Surrogate surrogate = makeSurrogate(parent.getSurrogate().getDatabase(), DBObjectType.CHRONICLE, id);
				ChronicleImpl.RawData data = new ChronicleImpl.RawData();
				data.setSurrogate(surrogate);
				int schema = rs.getInt(2);
				data.setCollection(parent);
				data.setSchema(schema == 0 ? null : makeSurrogate(parent.getSurrogate().getDatabase(), DBObjectType.SCHEMA, schema));
				data.setName(name);
				data.setDescription(rs.getString(3));
				chronicle = new ChronicleImpl(data);
				boolean permitted = check(Permission.READ, chronicle, false);
				if (!permitted)
					chronicle = null;
			}
			rs.close();
		} catch (SQLException e) {
			throw T2DBMsg.exception(e, E.E40123, name, parent.getName(true));
		} finally {
			select_entity_by_parent_and_name = close(select_entity_by_parent_and_name);
		}
		return chronicle;
	}
	
	private PreparedStatement select_entities_by_parent;
	private static final String SELECT_ENTITIES_BY_PARENT = 
		"select id, schema_id, name, descrip from " + DB.CHRONICLE + " where parent = ?";
	/**
	 * Return the collection of chronicles with a given direct parent.
	 * @param parent a chronicle
	 * @return a collection of chronicles, possibly empty, never null
	 * @throws T2DBException
	 */
	public Collection<Chronicle> getChroniclesByParent(Chronicle parent) throws T2DBException {
		Collection<Chronicle> result = new ArrayList<Chronicle>();
		if (check(Permission.DISCOVER, parent, false)) {
			try {
				select_entities_by_parent = open(SELECT_ENTITIES_BY_PARENT, parent, select_entities_by_parent);
				select_entities_by_parent.setInt(1, getIdOrZero(parent));
				ResultSet rs = select_entities_by_parent.executeQuery();
				Database database = parent.getSurrogate().getDatabase();
				while (rs.next()) {
					int id = rs.getInt(1);
					if (id == 0)
						throw T2DBMsg.exception(E.E40105, parent == null ? null : parent.toString(), rs.getString(3));
					Surrogate surrogate = makeSurrogate(database, DBObjectType.CHRONICLE, id);
					RawData data = new ChronicleImpl.RawData();
					data.setSurrogate(surrogate);
					int schema = rs.getInt(2);
					data.setCollection(parent);
					data.setSchema(schema == 0 ? null : makeSurrogate(database, DBObjectType.SCHEMA, schema));
					data.setName(rs.getString(3));
					data.setDescription(rs.getString(4));
					Chronicle chronicle = new ChronicleImpl(data);
					check(Permission.READ, chronicle);
					result.add(chronicle);
				}
				rs.close();
			} catch (SQLException e) {
				throw T2DBMsg.exception(e, E.E40122, parent.getName(true));
			} finally {
				select_entities_by_parent = close(select_entities_by_parent);
			}
		}
		return result;
	}
	
	private PreparedStatement[] sel_attibute_prop_in_ent;
	private static final String SEL_ATTRIBUTE_BY_PROP_IN_ENT = 
		"select chronicle, value, descrip from " + DB.ATTRIBUTE_VALUE + " where prop = ? and chronicle in (%s)";
	/**
	 * The method completes the attribute with the value found for one of the
	 * entities in the list and returns true. If no value was found, the method
	 * return false. If more than value was found, the one selected corresponds
	 * to the first entity encountered in the list.
	 * 
	 * @param chronicles a list of chronicles
	 * @param attribute an attribute 
	 * @return true if any value found
	 * @throws T2DBException
	 */
	public boolean getAttributeValue(List<Chronicle> chronicles, Attribute<?> attribute) throws T2DBException {
		// note: things would be easier here with Spring's SimpleJdbcTemplate and a named parameter
		
		if (sel_attibute_prop_in_ent == null)
			sel_attibute_prop_in_ent = new PreparedStatement[MAX_ENTITY_DEPTH];
		
		// index zero reserved for MAX_ENTITY_DEPTH elements or more
		int found = 0;
		int size = chronicles.size();
		if (size == 0)
			throw new IllegalArgumentException("entities list empty");
		try {
			String sql = null;
			PreparedStatement stmt = null;
			int[] ids = new int[size];
			DBObject dBObject = chronicles.get(0);
			if (size > MAX_ENTITY_DEPTH) {
				// "dynamic" statement
				stmt = sel_attibute_prop_in_ent[0];
				if (stmt != null && stmt.getParameterMetaData().getParameterCount() != size + 1)
					stmt = null;
				sql = String.format(SEL_ATTRIBUTE_BY_PROP_IN_ENT, repeat("?", ",", size));
				stmt = open(sql, dBObject, stmt);
				sel_attibute_prop_in_ent[0] = stmt;
			} else {
				stmt = sel_attibute_prop_in_ent[size];
				sql = String.format(SEL_ATTRIBUTE_BY_PROP_IN_ENT, repeat("?", ",", size));
				stmt = open(sql, dBObject, stmt);
				sel_attibute_prop_in_ent[size] = stmt;
			}
			stmt.setInt(1, getId(attribute.getProperty()));
			for (int i = 0; i < size; i++) {
				ids[i] = getId(chronicles.get(i));
				stmt.setInt(2 + i, ids[i]);
			}
			ResultSet rs = stmt.executeQuery();
			while(rs.next()) {
				int name = rs.getInt(1);
				if (name == 0)
					throw T2DBMsg.exception(E.E40106, sql);
				if (found == 0 || moreSpecific(ids, name, found)) {
					found = name;
					attribute.scan(rs.getString(2));
					String description = rs.getString(3);
					if (description.length() > 0)
						attribute.setDescription(description);
				}
			}
			rs.close();
		} catch (SQLException e) {
			throw T2DBMsg.exception(e, E.E40120, attribute.getProperty().getName());
		} finally {
			if (size > MAX_ENTITY_DEPTH)
				sel_attibute_prop_in_ent[0] = close(sel_attibute_prop_in_ent[0]);
			else if (size > 0)
				sel_attibute_prop_in_ent[size] = close(sel_attibute_prop_in_ent[size]);
		}
		return found > 0;
	}
	
	private PreparedStatement sel_entities_by_attribute;
	private static final String SEL_ENTITIES_BY_ATTRIBUTE = 
		"select chronicle from " + DB.ATTRIBUTE_VALUE + " where value = ? and prop = ?";
	/**
	 * Return a list of chronicles with a given value for a given property.
	 * 
	 * @param property a property
	 * @param value a value
	 * @param maxSize the maximum size of the result or 0 for no limit
	 * @return a list of chronicles, possibly empty, never null
	 * @throws T2DBException
	 */
	public <T>List<Chronicle> getChroniclesByAttributeValue(Property<T> property, T value, int maxSize) throws T2DBException {
		List<Chronicle> chronicles = new ArrayList<Chronicle>();
		String stringValue = property.getValueType().toString(value);
		try {
			sel_entities_by_attribute = open(SEL_ENTITIES_BY_ATTRIBUTE, property, sel_entities_by_attribute);
			sel_entities_by_attribute.setString(1, stringValue);
			sel_entities_by_attribute.setInt(2, getId(property));
			ResultSet rs = sel_entities_by_attribute.executeQuery();
			while (rs.next()) {
				Surrogate surrogate = makeSurrogate(property.getSurrogate().getDatabase(), DBObjectType.CHRONICLE, rs.getInt(1));
				Chronicle chronicle = new ChronicleImpl(surrogate);
				check(Permission.READ, chronicle);
				chronicles.add(chronicle);
				if (maxSize > 0 && chronicles.size() >= maxSize)
					break;
			}
			rs.close();
		} catch (SQLException e) {
			throw T2DBMsg.exception(e, E.E40119, property.getName(), value);
		} finally {
			sel_entities_by_attribute = close(sel_entities_by_attribute);
		}
		return chronicles;
	}
	
	/**
	 * Return true if a is before b in the list of ids else return false.
	 * 
	 * @param ids
	 * @param a
	 * @param b
	 * @return
	 */
	private boolean moreSpecific(int[] ids, int a, int b) {
		for(int id : ids) {
			if (a == id)
				return true;
			if (b == id)
				return false;
		}
		throw new RuntimeException("bug");
	}
	
	private String repeat(String s, String separator, int times) {
		if (times < 1)
			throw new IllegalArgumentException("times not positive");
		StringBuilder b = new StringBuilder();
		int i = times - 1;
		while (i-- > 0) {
			if (s != null)
				b.append(s);
			if (separator != null)
				b.append(separator);
		}
		if (s != null)
			b.append(s);
		return b.toString();
	}
	
	private PreparedStatement select_series_by_id;
	private static final String SELECT_SERIES_BY_ID = 
			"select chronicle, ssn from " + DB.SERIES + " where id = ?";
	/**
	 * Find a series corresponding to a surrogate.
	 * 
	 * @param surrogate a surrogate
	 * @return a series or null
	 * @throws T2DBException
	 */
	public <T>Series<T> getSeries(Surrogate surrogate) throws T2DBException {
		try {
			select_series_by_id = open(SELECT_SERIES_BY_ID, surrogate, select_series_by_id);
			int id = getId(surrogate);
			select_series_by_id.setInt(1, id);
			ResultSet rs = select_series_by_id.executeQuery();
			if (rs.next()) {
				Chronicle chronicle = new ChronicleImpl(makeSurrogate(surrogate.getDatabase(), DBObjectType.CHRONICLE, rs.getInt(1)));
				return new SeriesImpl<T>(chronicle, null, rs.getInt(2), surrogate);
			} else
				return null;
		} catch (SQLException e) {
			throw T2DBMsg.exception(e, E.E50119, surrogate.toString());
		} finally {
			select_series_by_id = close(select_series_by_id);
		}
	}
	
	private PreparedStatement select_series_by_entity_and_nr;
	private static final String SELECT_SERIES_BY_ENTITY_AND_NR = 
    	"select id from " + DB.SERIES + " where chronicle = ? and ssn = ?";
	private PreparedStatement select_series_by_entity;
	private static final String SELECT_SERIES_BY_ENTITY = 
    	"select id, ssn from " + DB.SERIES + " where chronicle = ? order by ssn";

	/**
	 * Return array of series in the positions corresponding to the
	 * requested numbers. These numbers are series numbers within the schema,
	 * not the series keys in the database. When a requested number in the array is
	 * non-positive, the corresponding series will be null in the result. A
	 * series can also be null when the requested number is positive but the
	 * series has no data and has not been set up in the database (and therefore
	 * has no series database key).
	 * 
	 * @param chronicle a chronicle
	 * @param names an array of simple names to plug into the series  
	 * @param numbers an array of numbers
	 * @return an array of series
	 * @throws T2DBException
	 */
	public <T>Series<T>[] getSeries(Chronicle chronicle, String[] names, int[] numbers) throws T2DBException {
		if (names.length != numbers.length)
			throw new IllegalArgumentException("names and numbers emtpy or unequally sized arrays");
		@SuppressWarnings("unchecked")
		Series<T>[] result = new SeriesImpl[numbers.length];
		int entityId = getId(chronicle);
		try {
			switch(numbers.length) {
			case 0:
				break;
			case 1:
				select_series_by_entity_and_nr = open(SELECT_SERIES_BY_ENTITY_AND_NR, chronicle, select_series_by_entity_and_nr);
				select_series_by_entity_and_nr.setInt(1, entityId);
				select_series_by_entity_and_nr.setInt(2, numbers[0]);
				ResultSet rs = select_series_by_entity_and_nr.executeQuery();
				if (rs.next()) {
					result[0] = new SeriesImpl<T>(chronicle, names[0], numbers[0], 
							makeSurrogate(chronicle.getSurrogate().getDatabase(), DBObjectType.SERIES, rs.getInt(1)));
				}
				// else	result[0] = null;
				break;
			default:
				select_series_by_entity = open(SELECT_SERIES_BY_ENTITY, chronicle, select_series_by_entity);
				select_series_by_entity.setInt(1, entityId);
				rs = select_series_by_entity.executeQuery();
				Map<Integer, Integer> index = new HashMap<Integer, Integer>(numbers.length);
				for (int i = 0; i < numbers.length; i++) {
					index.put(numbers[i], i);
				}
				while (rs.next()) {
					Integer i = index.get(rs.getInt(2));
					if (i != null)
						result[i] = new SeriesImpl<T>(chronicle, names[i], numbers[i], 
								makeSurrogate(chronicle.getSurrogate().getDatabase(), DBObjectType.SERIES, rs.getInt(1)));
				}
				rs.close();
				break;
			}
		} catch (SQLException e) {
			throw T2DBMsg.exception(e, E.E40121, chronicle.getName(true));
		} finally {
			select_series_by_entity_and_nr = close(select_series_by_entity_and_nr);
			select_series_by_entity = close(select_series_by_entity);
		}
		return result;
	}
	
}
