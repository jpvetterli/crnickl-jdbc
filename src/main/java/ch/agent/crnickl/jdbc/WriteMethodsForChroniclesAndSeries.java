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
import ch.agent.crnickl.api.AttributeDefinition;
import ch.agent.crnickl.api.Chronicle;
import ch.agent.crnickl.api.Schema;
import ch.agent.crnickl.api.Series;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.api.UpdatableChronicle;
import ch.agent.crnickl.api.UpdatableSeries;
import ch.agent.crnickl.impl.ChronicleUpdatePolicy;
import ch.agent.crnickl.impl.DatabaseBackend;
import ch.agent.crnickl.impl.Permission;

/**
 * A stateless object with methods providing write access to chronicles and
 * series.
 * 
 * @author Jean-Paul Vetterli
 */
public class WriteMethodsForChroniclesAndSeries extends	ReadMethodsForChroniclesAndSeries {

	public WriteMethodsForChroniclesAndSeries() {
	}

	private PreparedStatement create_entity;
	private static final String CREATE_ENTITY = 
			"insert into " + DB.CHRONICLE + "(parent, schema_id, name, descrip) values(?, ?, ?, ?)";
	/**
	 * Create a chronicle in the database.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param chronicle a chronicle
	 * @throws T2DBException
	 */
	public void createChronicle(Chronicle chronicle) throws T2DBException {
		Surrogate surrogate = null;
		Throwable cause = null;
		try {
			String name = chronicle.getName(false);
			((DatabaseBackend)chronicle.getSurrogate().getDatabase()).getNamingPolicy().checkSimpleName(name, false); // redundant but bugs exist
			String description = chronicle.getDescription(false);
			if (description == null)
				description = "";
			Chronicle collection = chronicle.getCollection();
			Schema schema = chronicle.getSchema(false);
			if (collection != null) {
				check(Permission.MODIFY, collection);
				if (schema != null && schema.equals(collection.getSchema(true)))
					schema = null; // don't repeat yourself
			}
			if (schema != null)
				check(Permission.READ, schema);
			create_entity = open(CREATE_ENTITY, chronicle, create_entity);
			create_entity.setInt(1, getIdOrZero(collection));
			create_entity.setInt(2, getIdOrZero(schema));
			create_entity.setString(3, name);
			create_entity.setString(4, description);
			surrogate = makeSurrogate(chronicle, executeAndGetNewId(create_entity));
		} catch (Exception e) {
			cause = e;
		} finally {
			create_entity = close(create_entity);
		}
		if (surrogate == null || cause != null)
			throw T2DBMsg.exception(cause, E.E40109, chronicle.getName(true));
		chronicle.getSurrogate().upgrade(surrogate);
	}
	
	private PreparedStatement delete_entity;
	private static final String DELETE_ENTITY = "delete from " + DB.CHRONICLE + " where id = ?";
	private PreparedStatement delete_entity_attributes;
	private static final String DELETE_ENTITY_ATTIBUTES = "delete from " + DB.ATTRIBUTE_VALUE + " where chronicle = ?";

	/**
	 * Delete a chronicle from the database. Also delete its attribute values
	 * and possibly other dependent objects. The chronicle update policy is
	 * supposed to forbid deleting when there are dependent chronicles or
	 * series, but to allow cascading delete of attribute values. Throw an
	 * exception if the operation cannot be done.
	 * 
	 * @param chronicle a chronicle
	 * @param policy a chronicle updating policy
	 * @throws T2DBException
	 */
	public void deleteChronicle(UpdatableChronicle chronicle, ChronicleUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		try {
			int id = getId(chronicle);
			check(Permission.MODIFY, chronicle);
			policy.willDelete(chronicle);
			done = policy.deleteChronicle(chronicle);

			// delete attributes first
			delete_entity_attributes = open(DELETE_ENTITY_ATTIBUTES, chronicle, delete_entity_attributes);
			delete_entity_attributes.setInt(1, id);
			delete_entity_attributes.execute();
			
			delete_entity = open(DELETE_ENTITY, chronicle, delete_entity);
			delete_entity.setInt(1, id);
			delete_entity.execute();
			done = delete_entity.getUpdateCount() > 0;
		} catch (Exception e) {
			cause = e;
		} finally {
			delete_entity = close(delete_entity);
			delete_entity_attributes = close(delete_entity_attributes);
		}
		if (!done || cause != null)
			throw T2DBMsg.exception(cause, E.E40110, chronicle.getName(true));
	}
	
	private PreparedStatement update_entity;
	private static final String UPDATE_ENTITY = 
		"update " + DB.CHRONICLE + " set name = ?, descrip = ? where id = ?";

	/**
	 * Update a chronicle. Currently only name and description can be updated.
	 * The schema and the parent chronicle cannot be updated. Throw an exception
	 * if the operation cannot be done.
	 * 
	 * @param chronicle
	 *            a chronicle
	 * @param policy
	 *            a chronicle updating policy
	 * @throws T2DBException
	 */
	public void updateChronicle(UpdatableChronicle chronicle, ChronicleUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		String name = chronicle.getName(false);
		String description = chronicle.getDescription(false);
		if (description == null)
			description = "";
		try {
			int id = getId(chronicle);
			check(Permission.MODIFY, chronicle);
			policy.willUpdate(chronicle);
			update_entity = open(UPDATE_ENTITY, chronicle, update_entity);
			update_entity.setString(1, name);
			update_entity.setString(2, description);
			update_entity.setInt(3, id);
			update_entity.execute();
			done = update_entity.getUpdateCount() > 0;
		} catch (Exception e) {
			cause = e;
		} finally {
			update_entity = close(update_entity);
		}
		if (!done || cause != null)
			throw T2DBMsg.exception(cause, E.E40111, chronicle.getName(true));
	}
	
	private PreparedStatement insert_attribute;
	private static final String INSERT_ATTRIBUTE = 
		"insert into " + DB.ATTRIBUTE_VALUE + "(chronicle, attrib, prop, value, descrip) values(?, ?, ?, ?, ?) ";
	/**
	 * Update a chronicle attribute.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param chronicle a chronicle
	 * @param def an attribute definition
	 * @param value a value 
	 * @param description a string
	 * @throws T2DBException
	 */
	public void updateAttribute(UpdatableChronicle chronicle, AttributeDefinition<?> def, String value, String description) throws T2DBException {
		boolean done = false;
		try {
			check(Permission.MODIFY, chronicle);
			insert_attribute = open(INSERT_ATTRIBUTE, chronicle, insert_attribute);
			insert_attribute.setInt(1, getId(chronicle));
			insert_attribute.setInt(2, def.getNumber());
			insert_attribute.setInt(3, getId(def.getProperty()));
			insert_attribute.setString(4, value);
			insert_attribute.setString(5, description);
			insert_attribute.execute();
			done = insert_attribute.getUpdateCount() > 0;
		} catch (SQLException e) {
			done = updateAttribute(e, chronicle, def, value, description);
		} finally {
			insert_attribute = close(insert_attribute);
		}
		if (!done)
			throw T2DBMsg.exception(E.E40112, chronicle.getName(true), def.getNumber());
	}
	
	private PreparedStatement update_attribute;
	private static final String UPDATE_ATTRIBUTE = 
		"update " + DB.ATTRIBUTE_VALUE + " set value = ?, descrip = ? where chronicle = ? and prop = ?";
	private boolean updateAttribute(SQLException originalException, UpdatableChronicle entity, AttributeDefinition<?> def, String value, String description) throws T2DBException {
		boolean done = false;
		try {
			check(Permission.MODIFY, entity);
			update_attribute = open(UPDATE_ATTRIBUTE, entity, update_attribute);
			update_attribute.setString(1, value);
			update_attribute.setString(2, description);
			update_attribute.setInt(3, getId(entity));
			update_attribute.setInt(4, getId(def.getProperty()));
			update_attribute.execute();
			done = update_attribute.getUpdateCount() > 0;
		} catch (Exception e) {
			throw T2DBMsg.exception(originalException, E.E40113, entity.getName(true), def.getNumber());
		} finally {
			update_attribute = close(update_attribute);
		}
		return done;
	}
	
	private PreparedStatement delete_attribute;
	private static final String DELETE_ATTRIBUTE = 
		"delete from " + DB.ATTRIBUTE_VALUE + " where chronicle = ? and prop = ?";
	/**
	 * Delete an attribute value from a chronicle.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param chronicle a chronicle
	 * @param def an attribute definition
	 * @throws T2DBException
	 */
	public void deleteAttribute(Chronicle chronicle, AttributeDefinition<?> def) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		try {
			check(Permission.MODIFY, chronicle);
			delete_attribute = open(DELETE_ATTRIBUTE, chronicle, delete_attribute);
			delete_attribute.setInt(1, getId(chronicle));
			delete_attribute.setInt(2, getId(def.getProperty()));
			delete_attribute.execute();
			done = delete_attribute.getUpdateCount() > 0;
		} catch (Exception e) {
			cause = e;
		} finally {
			delete_attribute = close(delete_attribute);
		}
		if (!done || cause != null)
			throw T2DBMsg.exception(cause, E.E40114, chronicle.getName(true), def.getNumber());
	}

	private PreparedStatement create_series;
	private static final String CREATE_SERIES = 
		"insert into " + DB.SERIES + "(chronicle, ssn) values(?, ?)";

	/**
	 * Create an empty series. Throw an exception if the operation cannot be
	 * done.
	 * 
	 * @param series
	 *            a series
	 * @throws T2DBException
	 */
	public void createSeries(Series<?> series) throws T2DBException {
		Throwable cause = null;
		Surrogate surrogate = null;
		Chronicle chronicle = series.getChronicle();
		try {
			check(Permission.MODIFY, chronicle, true);
			create_series = open(CREATE_SERIES, series, create_series);
			create_series.setInt(1, getId(chronicle));
			create_series.setInt(2, series.getNumber());
			surrogate = makeSurrogate(series, executeAndGetNewId(create_series));
		} catch (Exception e) {
			cause = e;
		} finally {
			create_series = close(create_series);
		}
		if (surrogate == null || cause != null)
			throw T2DBMsg.exception(cause, E.E50111, series.getName(true));
		series.getSurrogate().upgrade(surrogate);
	}
	
	private PreparedStatement delete_series;
	private static final String DELETE_SERIES = "delete from " + DB.SERIES + " where id = ?";

	/**
	 * Delete a series. The policy typically forbids to delete a series which is
	 * not empty. Throw an exception if the operation cannot be done.
	 * 
	 * @param series
	 *            a series
	 * @param policy
	 *            a chronicle update policy
	 * @throws T2DBException
	 */
	public void deleteSeries(UpdatableSeries<?> series, ChronicleUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		try {
			int id = getId(series);
			check(Permission.MODIFY, series);
			policy.willDelete(series);
			done = policy.deleteSeries(series);
			delete_series = open(DELETE_SERIES, series, delete_series);
			delete_series.setInt(1, id);
			delete_series.execute();
			if (delete_series.getUpdateCount() > 0)
				done = true;
		} catch (Exception e) {
			cause = e;
		} finally {
			delete_series = close(delete_series);
		}
		if (!done || cause != null)
			throw T2DBMsg.exception(cause, E.E50112, series.getName(true));
	}

}
