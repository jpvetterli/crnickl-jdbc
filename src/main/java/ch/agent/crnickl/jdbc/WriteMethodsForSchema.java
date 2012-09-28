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

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.api.AttributeDefinition;
import ch.agent.crnickl.api.DBObjectType;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Property;
import ch.agent.crnickl.api.Schema;
import ch.agent.crnickl.api.SeriesDefinition;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.api.UpdatableSchema;
import ch.agent.crnickl.impl.AttributeDefinitionImpl;
import ch.agent.crnickl.impl.DatabaseBackend;
import ch.agent.crnickl.impl.Permission;
import ch.agent.crnickl.impl.UpdatableSchemaImpl;
import ch.agent.crnickl.impl.UpdatableSchemaVisitor;
import ch.agent.crnickl.jdbc.T2DBJMsg.J;

/**
 * A stateless object with methods providing write access to schemas.
 *  
 * @author Jean-Paul Vetterli
 * @version 1.0.0
 */
public class WriteMethodsForSchema extends ReadMethodsForSchema {

	private static class Visitor implements UpdatableSchemaVisitor {

		private WriteMethodsForSchema m;

		private Visitor(WriteMethodsForSchema methods) {
			this.m = methods;
		}

		@Override
		public void visit(UpdatableSchema schema, SeriesDefinition def,
				SeriesDefinition original) throws T2DBException {
			if (def == null) {
				// delete series definition
				m.deleteSchemaComponents(schema, original.getNumber());
			} else {
				if (def.isErasing()) {
					AttributeDefinitionImpl<String> attribDef = new AttributeDefinitionImpl<String>(
							DatabaseBackend.MAGIC_NAME_NR, null, null);
					attribDef.edit();
					attribDef.setErasing(true);
					if (original == null) {
						// create erasing series
						m.createSchemaComponent(schema, def.getNumber(), null, attribDef);
					} else {
						// update series to erasing
						if (!original.isErasing())
							m.updateSchemaComponent(schema, def.getNumber(), null, attribDef);
					}
				}
				// description taken care of in the other visit() method
			}
		}

		@Override
		public void visit(UpdatableSchema schema, SeriesDefinition seriesDef,
				AttributeDefinition<?> attrDef,
				AttributeDefinition<?> origAttrDef) throws T2DBException {
			int seriesNr = seriesDef == null ? 0 : seriesDef.getNumber();
			if (attrDef == null) {
				// delete attribute definition
				m.deleteSchemaComponent(schema, seriesNr, origAttrDef.getNumber());
			} else {
				String description = null;
				if (seriesNr != 0) {
					if (attrDef.getNumber() == DatabaseBackend.MAGIC_NAME_NR)
						description = seriesDef.getDescription();
				}
				if (origAttrDef == null) {
					// create attribute definition
					m.createSchemaComponent(schema, seriesNr, description, attrDef);
				} else {
					// update attribute definition
					m.updateSchemaComponent(schema, seriesNr, description, attrDef);
				}
			}
		}
	}
	
	private UpdatableSchemaVisitor visitor;
	
	public WriteMethodsForSchema() {
		visitor = new Visitor(this);
	}
	
	private PreparedStatement create_schema;
	private static final String CREATE_SCHEMA = "insert into " + DB.SCHEMA_NAME + "(parent, label) values(?, ?)";
	/**
	 * Create an empty schema in the database.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param schema a schema
	 * @throws T2DBException
	 */
	public void createSchema(UpdatableSchema schema) throws T2DBException {
		Surrogate surrogate = null;
		Throwable cause = null;
		try {
			Schema base = schema.getBase();
			check(Permission.CREATE, schema);
			if (base != null)
				check(Permission.READ, base);
			create_schema = open(CREATE_SCHEMA, schema, create_schema);
			create_schema.setInt(1, getIdOrZero(base));
			create_schema.setString(2, schema.getName());
			surrogate = makeSurrogate(schema, executeAndGetNewId(create_schema));
			schema.getSurrogate().upgrade(surrogate); 
			updateSchemaComponents(schema);
		} catch (Exception e) {
			cause = e;
		} finally {
			create_schema = close(create_schema);
		}
		if (surrogate == null || cause != null)
			throw T2DBJMsg.exception(cause, J.J30122, schema.getName());
	}
	
	private PreparedStatement delete_schema;
	private PreparedStatement delete_schema_components;
	private static final String DELETE_SCHEMA = "delete from " + DB.SCHEMA_NAME + " where id = ?";
	private static final String DELETE_SCHEMA_COMPONENTS = "delete from " + DB.SCHEMA_ITEM + " where id = ?";
	/**
	 * Delete a schema from the database.
	 * Throw an exception if the operation fails.
	 * 
	 * @param schema a schema
	 * @throws T2DBException
	 */
	public void deleteSchema(UpdatableSchema schema) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		try {
			check(Permission.MODIFY, schema);
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
	private void createSchemaComponent(UpdatableSchema schema, int seriesNr, String description, AttributeDefinition<?> def) throws T2DBException {
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
	 * Update the schema in the database.
	 * Throw an exception if the operation cannot be done.
	 * 
	 * @param schema a schema
	 * @return true if the schema was updated
	 * @throws T2DBException
	 */
	public boolean updateSchema(UpdatableSchema schema) throws T2DBException {
		boolean done = false;
		Throwable cause = null;
		RawSchema rawSchema = getRawSchema(schema.getSurrogate());
		boolean nameEdited = nameEdited(schema, rawSchema);
		boolean baseEdited = baseEdited(schema, rawSchema);
		if (nameEdited || baseEdited) {
			try {
				Schema base = schema.getBase();
				check(Permission.MODIFY, schema);
				if (baseEdited && base != null)
					check(Permission.READ, base);
				update_schema = open(UPDATE_SCHEMA, schema, update_schema);
				update_schema.setString(1, schema.getName());
				update_schema.setInt(2, getIdOrZero(base));
				update_schema.setInt(3, getId(schema));
				update_schema.execute();
				done = update_schema.getUpdateCount() > 0;
			} catch (Exception e) {
				cause = e;
			} finally {
				update_schema = close(update_schema);
			}
		}
		try {
			done |= updateSchemaComponents(schema);
		} catch (Exception e) {
			cause = e;
		} 
		if (!done || cause != null)
			throw T2DBJMsg.exception(cause, J.J30126, schema);
		return done;
	}
	
	private boolean updateSchemaComponents(UpdatableSchema schema) throws T2DBException {
		return ((UpdatableSchemaImpl) schema).visit(visitor) > 0;
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
	private void deleteSchemaComponent(UpdatableSchema schema, int seriesNr, int attribNr) throws T2DBException {
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
	private void deleteSchemaComponents(UpdatableSchema schema, int seriesNr) throws T2DBException {
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
	private void updateSchemaComponent(UpdatableSchema schema, int seriesNr, String description, AttributeDefinition<?> def) throws T2DBException {
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
	private static final String FIND_ENTITY_WITH_SCHEMA = "select id from " + DB.CHRONICLE + " where schema_id = ?";
	/**
	 * Find all chronicles referencing the schema. 
	 * This looks like a "reading" method but is used in the context of schema updating.
	 * 
	 * @param schema a schema
	 * @return a collection of chronicle surrogates
	 * @throws T2DBException
	 */
	public Collection<Surrogate> findChronicles(Schema schema) throws T2DBException {
		Collection<Surrogate> result = new ArrayList<Surrogate>();
		Database database = schema.getSurrogate().getDatabase();
		try {
			find_entity_with_schema = open(FIND_ENTITY_WITH_SCHEMA, database, find_entity_with_schema);
			find_entity_with_schema.setInt(1, getId(schema));
			ResultSet rs = find_entity_with_schema.executeQuery();
			while (rs.next()) {
				result.add(makeSurrogate(database, DBObjectType.CHRONICLE, rs.getInt(1)));
			}
		} catch (Exception e) {
			throw T2DBJMsg.exception(e, J.J30117);
		} finally {
			find_entity_with_schema = close(find_entity_with_schema);
		}
		return result;
	}
	
	private PreparedStatement find_entity_with_property;
	private static final String FIND_ENTITY_WITH_PROPERTY = "select chronicle from " + DB.ATTRIBUTE_VALUE + " where prop = ?";
	/**
	 * Find all chronicles with an explicit attribute value for a given property and schema. 
	 * This looks like a "reading" method but is used in the context of schema updating.
	 * 
	 * @param property a property
	 * @param schema a schema
	 * @return a collection of chronicle surrogates
	 * @throws T2DBException
	 */
	public Collection<Surrogate> findChronicles(Property<?> property, Schema schema) throws T2DBException {
		Collection<Surrogate> result = new ArrayList<Surrogate>();
		Database database = property.getSurrogate().getDatabase();
		try {
			find_entity_with_property = open(FIND_ENTITY_WITH_PROPERTY, property, find_entity_with_property);
			find_entity_with_property.setInt(1, getId(property));
			ResultSet rs = find_entity_with_property.executeQuery();
			while(rs.next()) {
				Surrogate entityKey = makeSurrogate(database, DBObjectType.CHRONICLE, rs.getInt(1));
				Schema s = database.getChronicle(entityKey).getSchema(true);
				if (s.dependsOnSchema(schema))
					result.add(entityKey);
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
	 * Find all chronicles with actual series depending from a given schema.
	 * This looks like a "reading" method but is used in the context of schema updating.
	 * 
	 * @param ss a series definition
	 * @param schemas a schema
	 * @return a collection of chronicle surrogates
	 * @throws T2DBException
	 */
	public Collection<Surrogate> findChronicles(SeriesDefinition ss, Schema schema) throws T2DBException {
		Collection<Surrogate> result = new ArrayList<Surrogate>();
		Database database = schema.getSurrogate().getDatabase();
		try {
			find_entity_with_series = open(FIND_ENTITY_WITH_SERIES, database, find_entity_with_series);
			find_entity_with_series.setInt(1, ss.getNumber());
			ResultSet rs = find_entity_with_series.executeQuery();
			while (rs.next()) {
				Surrogate entityKey = makeSurrogate(database, DBObjectType.CHRONICLE, rs.getInt(1));
				Schema s = database.getChronicle(entityKey).getSchema(true);
				if (s.dependsOnSchema(schema))
					result.add(entityKey);
			}
		} catch (Exception e) {
			throw T2DBJMsg.exception(e, J.J30117);
		} finally {
			find_entity_with_series = close(find_entity_with_series);
		}
		return result;
	}

}
