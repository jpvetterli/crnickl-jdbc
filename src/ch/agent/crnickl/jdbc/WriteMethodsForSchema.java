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
 * Type: WriteMethodsForSchema
 * Version: 1.0.0
 */
package ch.agent.crnickl.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.api.AttributeDefinition;
import ch.agent.crnickl.api.DBObjectType;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Property;
import ch.agent.crnickl.api.Schema;
import ch.agent.crnickl.api.SeriesDefinition;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.api.UpdatableSchema;
import ch.agent.crnickl.impl.Permission;
import ch.agent.crnickl.impl.SchemaUpdatePolicy;
import ch.agent.crnickl.impl.UpdatableSchemaImpl;
import ch.agent.crnickl.jdbc.T2DBJMsg.J;

/**
 * A stateless object with methods providing write access to schemas.
 *  
 * @author Jean-Paul Vetterli
 * @version 1.0.0
 */
public class WriteMethodsForSchema extends ReadMethodsForSchema {

	public WriteMethodsForSchema() {
	}
	
	private PreparedStatement create_schema;
	private static final String CREATE_SCHEMA = "insert into " + DB.SCHEMA_NAME + "(parent, label) values(?, ?)";
	/**
	 * Create an empty schema in the database.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param schema a schema
	 * @param base a base schema or null
	 * @throws T2DBException
	 */
	public void createSchema(UpdatableSchemaImpl schema, Schema base) throws T2DBException {
		Surrogate surrogate = null;
		Throwable cause = null;
		try {
			check(Permission.CREATE, schema);
			if (base != null)
				check(Permission.READ, base);
			create_schema = open(CREATE_SCHEMA, schema, create_schema);
			create_schema.setInt(1, getIdOrZero(base));
			create_schema.setString(2, schema.getName());
			surrogate = makeSurrogate(schema, executeAndGetNewId(create_schema));
		} catch (Exception e) {
			cause = e;
		} finally {
			create_schema = close(create_schema);
		}
		if (surrogate == null || cause != null)
			throw T2DBJMsg.exception(cause, J.J30122, schema.getName());
		schema.getSurrogate().upgrade(surrogate); 
	}
	
	private PreparedStatement delete_schema;
	private PreparedStatement delete_schema_components;
	private static final String DELETE_SCHEMA = "delete from " + DB.SCHEMA_NAME + " where id = ?";
	private static final String DELETE_SCHEMA_COMPONENTS = "delete from " + DB.SCHEMA_ITEM + " where id = ?";
	/**
	 * Delete a schema from the database.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param schema a schema
	 * @param policy a schema udpdating policy
	 * @throws T2DBException
	 */
	public void deleteSchema(UpdatableSchemaImpl schema, SchemaUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		try {
			check(Permission.MODIFY, schema);
			policy.willDelete(schema);
			delete_schema_components = open(DELETE_SCHEMA_COMPONENTS, schema, delete_schema_components);
			int id = getId(schema);
			delete_schema_components.setInt(1, id);
			delete_schema_components.execute();
			delete_schema = open(DELETE_SCHEMA, schema, delete_schema);
			delete_schema.setInt(1, id);
			delete_schema.execute();
			done = delete_schema.getUpdateCount() > 0;
		} catch (Exception e) {
			cause = e;
		} finally {
			delete_schema_components = close(delete_schema_components);
			delete_schema = close(delete_schema);
		}
		if (!done || cause != null)
			throw T2DBJMsg.exception(cause, J.J30123, schema.getName());
	}

	private PreparedStatement create_schema_component;
	private static final String CREATE_SCHEMA_COMPONENT = 
		"insert into " + DB.SCHEMA_ITEM + "(id, ssn, attrib, prop, value, descrip) " +
		"values(?, ?, ?, ?, ?, ?)";
	/**
	 * Create an attribute definition in the database.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param schema a schema
	 * @param seriesNr a series number
	 * @param description a string
	 * @param def an attribute definition
	 * @throws T2DBException
	 */
	public void createSchemaComponent(UpdatableSchema schema, int seriesNr, String description, AttributeDefinition<?> def) throws T2DBException {
		boolean done = false;
		try {
			check(Permission.MODIFY, schema);
			create_schema_component = open(CREATE_SCHEMA_COMPONENT, schema, create_schema_component);
			create_schema_component.setInt(1, getId(schema));
			create_schema_component.setInt(2, seriesNr);
			create_schema_component.setInt(3, def.getNumber());
			if (def.isErasing()) {
				create_schema_component.setInt(4, 0);
				create_schema_component.setString(5, "");
				create_schema_component.setString(6, "");
			} else {
				Property<?> prop = def.getProperty();
				if (prop == null)
					throw T2DBJMsg.exception(J.J30130);
				create_schema_component.setInt(4, getId(prop));
				String value = def.getProperty().getValueType().toString(def.getValue());
				create_schema_component.setString(5, value == null ? "" : value);
				create_schema_component.setString(6, description == null ? "" : description);
			}
			create_schema_component.execute();
			done = create_schema_component.getUpdateCount() > 0;
		} catch (Exception e) {
			if (!done) {
				if (seriesNr == 0)
					throw T2DBJMsg.exception(e, J.J30124, def.getNumber());
				else
					throw T2DBJMsg.exception(e, J.J30125, def.getNumber(), seriesNr);
			}
		} finally {
			create_schema_component = close(create_schema_component);
		}
	}

	private PreparedStatement update_schema;
	private static final String UPDATE_SCHEMA = 
		"update " + DB.SCHEMA_NAME + " set label = ?, parent = ? where id = ?";
	/**
	 * Update the basic schema setup in the database.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param schema a schema
	 * @param base a base schema
	 * @param name a string
	 * @return true if the schema was updated
	 * @throws T2DBException
	 */
	public boolean updateSchema(UpdatableSchema schema, UpdatableSchema base, String name) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		RawSchema rawSchema = getRawSchema(schema.getSurrogate());
		boolean nameEdited = nameEdited(schema, rawSchema);
		boolean baseEdited = baseEdited(schema, rawSchema);
		if (nameEdited || baseEdited) {
			try {
				check(Permission.MODIFY, schema);
				UpdatableSchema previousBase = ((UpdatableSchemaImpl) schema).getPreviousBase();
				if (schema.getBase() != null && !schema.getBase().equals(previousBase))
					check(Permission.READ, schema.getBase());
				update_schema = open(UPDATE_SCHEMA, schema, update_schema);
				update_schema.setString(1, name);
				update_schema.setInt(2, getIdOrZero(base));
				update_schema.setInt(3, getId(schema));
				update_schema.execute();
				done = update_schema.getUpdateCount() > 0;
			} catch (Exception e) {
				cause = e;
			} finally {
				update_schema = close(update_schema);
			}
			if (!done || cause != null)
				throw T2DBJMsg.exception(cause, J.J30126, schema);
		}
		return done;
	}
	
	private PreparedStatement delete_schema_by_attribute;
	private static final String DELETE_SCHEMA_BY_ATTRIBUTE = 
		"delete from " + DB.SCHEMA_ITEM + " where id = ? and ssn = ? and attrib = ?";
	/**
	 * Delete an attribute definition from the database.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param schema a schema
	 * @param seriesNr a series number
	 * @param attribNr an attribute number
	 * @throws T2DBException
	 */
	public void deleteSchemaComponent(UpdatableSchema schema, int seriesNr, int attribNr) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		try {
			check(Permission.MODIFY, schema);
			delete_schema_by_attribute = open(DELETE_SCHEMA_BY_ATTRIBUTE, schema, delete_schema_by_attribute);
			delete_schema_by_attribute.setInt(1, getId(schema));
			delete_schema_by_attribute.setInt(2, seriesNr);
			delete_schema_by_attribute.setInt(3, attribNr);
			delete_schema_by_attribute.execute();
			if (delete_schema_by_attribute.getUpdateCount() > 0)
				done = true;
		} catch (Exception e) {
			cause = e;
		} finally {
			delete_schema_by_attribute = close(delete_schema_by_attribute);
		}
		if (!done || cause != null)
			throw T2DBJMsg.exception(cause, J.J30128, schema, seriesNr, attribNr);
	}
	
	private PreparedStatement delete_schema_by_series;
	private static final String DELETE_SCHEMA_BY_SERIES = 
		"delete from " + DB.SCHEMA_ITEM + " where id = ? and ssn = ?";
	/**
	 * Delete a series definition from the database.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param schema a schema
	 * @param seriesNr a series number
	 * @throws T2DBException
	 */
	public void deleteSchemaComponents(UpdatableSchema schema, int seriesNr) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		try {
			check(Permission.MODIFY, schema);
			delete_schema_by_series = open(DELETE_SCHEMA_BY_SERIES, schema, delete_schema_by_series);
			delete_schema_by_series.setInt(1, getId(schema));
			delete_schema_by_series.setInt(2, seriesNr);
			delete_schema_by_series.execute();
			if (delete_schema_by_series.getUpdateCount() > 0)
				done = true;
		} catch (Exception e) {
			cause = e;
		} finally {
			delete_schema_by_series = close(delete_schema_by_series);
		}
		if (!done || cause != null)
			throw T2DBJMsg.exception(cause, J.J30129, schema, seriesNr);
	}
	
	private PreparedStatement update_schema_by_attribute;
	private static final String UPDATE_SCHEMA_BY_ATTRIBUTE = 
		"update " + DB.SCHEMA_ITEM + " set value = ?, descrip = ? where id = ? and ssn = ? and attrib = ?";
	/**
	 * Update an attribute definition in the database.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param schema a schema
	 * @param seriesNr a series number
	 * @param description a string
	 * @param def an attribute definition
	 * @throws T2DBException
	 */
	public void updateSchemaComponent(UpdatableSchema schema, int seriesNr, String description, AttributeDefinition<?> def) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		try {
			check(Permission.MODIFY, schema);
			update_schema_by_attribute = open(UPDATE_SCHEMA_BY_ATTRIBUTE, schema, update_schema_by_attribute);
			String value = def.getProperty().getValueType().toString(def.getValue());
			update_schema_by_attribute.setString(1, value == null ? "" : value);
			update_schema_by_attribute.setString(2, description == null ? "" : description);
			update_schema_by_attribute.setInt(3, getId(schema));
			update_schema_by_attribute.setInt(4, seriesNr);
			update_schema_by_attribute.setInt(5, def.getNumber());
			update_schema_by_attribute.execute();
			if(update_schema_by_attribute.getUpdateCount() > 0)
				done = true;
		} catch (Exception e) {
			cause = e;
		} finally {
			update_schema_by_attribute = close(update_schema_by_attribute);
		}
		if (!done || cause != null)
			throw T2DBJMsg.exception(cause, J.J30127, schema, seriesNr, def.getNumber());
	}
	
	private boolean nameEdited(UpdatableSchema schema, RawSchema current) {
		return !schema.getName().equals(current.getName());
	}
	
	private boolean baseEdited(UpdatableSchema schema, RawSchema current) {
		UpdatableSchema base = schema.getBase();
		int editedBaseId = getIdOrZero(base);
		int currentBaseId = current.getParent();
		return editedBaseId != currentBaseId;
	}
	
	private PreparedStatement find_entity_with_schema;
	private static final String FIND_ENTITY_WITH_SCHEMA = "select id from " + DB.CHRONICLE + " where schema_id != 0";
	/**
	 * Find all chronicles referencing one of the schemas. 
	 * This looks like a "reading" method but is used in the context of schema updating.
	 * 
	 * @param schemas a collection of schemas
	 * @return a collection of chronicle surrogates
	 * @throws T2DBException
	 */
	public Collection<Surrogate> findChronicles(Collection<UpdatableSchema> schemas) throws T2DBException {
		Collection<Surrogate> result = new ArrayList<Surrogate>();
		if (schemas.size() > 0) {
			Set<Integer> schemaIds = new HashSet<Integer>(schemas.size());
			for (UpdatableSchema s : schemas) {
				schemaIds.add(getId(s));
			}
			Database database = schemas.iterator().next().getSurrogate().getDatabase();
			try {
				find_entity_with_schema = open(FIND_ENTITY_WITH_SCHEMA, database, find_entity_with_schema);
				ResultSet rs = find_entity_with_schema.executeQuery();
				while (rs.next()) {
					if (schemaIds.contains(rs.getInt(1))) {
						result.add(makeSurrogate(database, DBObjectType.CHRONICLE, rs.getInt(1)));
					}
				}
			} catch (Exception e) {
				throw T2DBJMsg.exception(e, J.J30117);
			} finally {
				find_entity_with_schema = close(find_entity_with_schema);
			}
		}
		return result;
	}
	
	private PreparedStatement find_entity_with_property;
	private static final String FIND_ENTITY_WITH_PROPERTY = "select chronicle from " + DB.ATTRIBUTE_VALUE + " where prop = ?";
	/**
	 * Find all chronicles with an explicit attribute value for a given property and schemas. 
	 * This looks like a "reading" method but is used in the context of schema updating.
	 * 
	 * @param property a property
	 * @param schemas a collection of schemas
	 * @return a collection of chronicle surrogates
	 * @throws T2DBException
	 */
	public Collection<Surrogate> findChronicles(Property<?> property, Collection<UpdatableSchema> schemas) throws T2DBException {
		Collection<Surrogate> result = new ArrayList<Surrogate>();
		Database database = property.getSurrogate().getDatabase();
		// expensive
		Set<Integer> schemaIds = new HashSet<Integer>(schemas.size());
		for(UpdatableSchema s : schemas) {
			schemaIds.add(getId(s));
		}
		try {
			find_entity_with_property = open(FIND_ENTITY_WITH_PROPERTY, property, find_entity_with_property);
			find_entity_with_property.setInt(1, getId(property));
			ResultSet rs = find_entity_with_property.executeQuery();
			while(rs.next()) {
				if (schemaIds.contains(rs.getInt(1))) {
					Surrogate entityKey = makeSurrogate(database, DBObjectType.CHRONICLE, rs.getInt(1));
					Schema schema = database.getChronicle(entityKey).getSchema(true); // hope caching is on
					if (schemaIds.contains(getId(schema)))
						result.add(entityKey);
				}
			}
		} catch (Exception e) {
			throw T2DBJMsg.exception(e, J.J30117);
		} finally {
			find_entity_with_property = close(find_entity_with_property);
		}
		return result;
	}

	private PreparedStatement find_entity_with_series;
	private static final String FIND_ENTITY_WITH_SERIES = "select chronicle from " + DB.SERIES + " where ssn = ?";
	/**
	 * Find all chronicles with actual series in a collection of schemas.
	 * This looks like a "reading" method but is used in the context of schema updating.
	 * 
	 * @param ss a series definition
	 * @param schemas a collection of schemas
	 * @return a collection of chronicle surrogates
	 * @throws T2DBException
	 */
	public Collection<Surrogate> findChronicles(SeriesDefinition ss, Collection<UpdatableSchema> schemas) throws T2DBException {
		Collection<Surrogate> result = new ArrayList<Surrogate>();
		if (schemas.size() > 0) {
			// expensive method
			Set<Integer> schemaIds = new HashSet<Integer>(schemas.size());
			for (UpdatableSchema s : schemas) {
				schemaIds.add(getId(s));
			}
			Database database = schemas.iterator().next().getSurrogate().getDatabase();
			try {
				find_entity_with_series = open(FIND_ENTITY_WITH_SERIES, database, find_entity_with_series);
				find_entity_with_series.setInt(1, ss.getNumber());
				ResultSet rs = find_entity_with_series.executeQuery();
				while (rs.next()) {
					if (schemaIds.contains(rs.getInt(1))) {
						Surrogate entityKey = makeSurrogate(database, DBObjectType.CHRONICLE, rs.getInt(1));
						Schema schema = database.getChronicle(entityKey).getSchema(true); // hope caching is on
						if (schemaIds.contains(getId(schema)))
							result.add(entityKey);
					}
				}
			} catch (Exception e) {
				throw T2DBJMsg.exception(e, J.J30117);
			} finally {
				find_entity_with_series = close(find_entity_with_series);
			}
		}
		return result;
	}	
	
}
