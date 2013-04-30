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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg;
import ch.agent.crnickl.T2DBMsg.E;
import ch.agent.crnickl.api.AttributeDefinition;
import ch.agent.crnickl.api.DBObjectType;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Property;
import ch.agent.crnickl.api.SeriesDefinition;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.api.UpdatableSchema;
import ch.agent.crnickl.impl.AttributeDefinitionImpl;
import ch.agent.crnickl.impl.DatabaseBackend;
import ch.agent.crnickl.impl.SeriesDefinitionImpl;
import ch.agent.crnickl.impl.UpdatableSchemaImpl;

/**
 * A stateless object with methods providing read access to schemas.
 *  
 * @author Jean-Paul Vetterli
 */
public class ReadMethodsForSchema extends JDBCDatabaseMethods {

	public ReadMethodsForSchema() {
	}
	
	/**
	 * Find a schema corresponding to a surrogate.
	 * 
	 * @param surrogate a surrogate
	 * @return a schema or null
	 * @throws T2DBException
	 */
	public UpdatableSchema getSchema(Surrogate surrogate) throws T2DBException {
		UpdatableSchema schema = null;
		RawSchema rawSchema = getRawSchema(surrogate);
		if (rawSchema != null)
			schema = getSchema(surrogate.getDatabase(), rawSchema, null);
		return schema;
	}
	
	/**
	 * Find a collection of schema surrogates with labels matching a pattern.
	 * 
	 * @param database a database
	 * @param pattern a simple pattern where "*" stands for zero or more characters
	 * @return a collection of schema surrogates
	 * @throws T2DBException
	 */
	public Collection<Surrogate> getSchemaSurrogateList(Database database, String pattern) throws T2DBException {
		Collection<RawSchema> rawSchemas = getRawSchemas(database, pattern);
		Collection<Surrogate> result = new ArrayList<Surrogate>(rawSchemas.size());
		for(RawSchema rs : rawSchemas) {
			result.add(makeSurrogate(database, DBObjectType.SCHEMA, rs.getId()));
		}
		return result;
	}		
	
	/* PRIVATE AREA */
	
	/**
	 * A raw schema object keeps track of the id, the name and the parent of a
	 * schema.
	 * 
	 */
	protected class RawSchema {
		private String name;
		private int id;
		private int parent;
		/**
		 * Construct a raw schema.
		 * 
		 * @param name a string 
		 * @param id a positive number
		 * @param parent a non-negative number
		 */
		public RawSchema(String name, int id, int parent) {
			this.name = name;
			this.id = id;
			this.parent = parent;
		}
		/**
		 * Return the name.
		 * 
		 * @return the name
		 */
		protected String getName() {
			return name;
		}
		/**
		 * Return the id.
		 * 
		 * @return the id
		 */
		protected int getId() {
			return id;
		}
		/**
		 * Return the parent.
		 * 
		 * @return the parent
		 */
		protected int getParent() {
			return parent;
		}
	}
	
	private class RawSchemaComponent {
		private int seriesNr;
		private int attribNr;
		private int propId;
		private String value;
		private String description;
		public RawSchemaComponent(int seriesNr, int attribNr, int propId, String value, String description) {
			this.seriesNr = seriesNr;
			this.attribNr = attribNr;
			this.propId = propId;
			this.value = value;
			this.description = description;
		}
	}
	
	private class RawSchemaComponents {
		private List<RawSchemaComponent> attributes;
		private List<List<RawSchemaComponent>> series;
		public RawSchemaComponents() {
			attributes = new ArrayList<RawSchemaComponent>();
			series = new ArrayList<List<RawSchemaComponent>>();
		}
		public List<RawSchemaComponent> getSeries(int id) {
			List<RawSchemaComponent> list = null;
			for (List<RawSchemaComponent> seriesAttrList : series) {
				if (seriesAttrList.size() > 0 && seriesAttrList.get(0).seriesNr == id) {
					list = seriesAttrList;
					break;
				}
			}
			if (list == null) {
				list = new ArrayList<RawSchemaComponent>();
				series.add(list);
			}
			return list;
		}
	}
	
	private PreparedStatement select_schema_by_pattern;
	private static final String SELECT_SCHEMA_BY_PATTERN = 
		"select id, parent, label from " + DB.SCHEMA_NAME + " " +
		"where label like ? order by label";
	private Collection<RawSchema> getRawSchemas(Database database, String pattern) throws T2DBException {
		select_schema_by_pattern = open(SELECT_SCHEMA_BY_PATTERN, database, select_schema_by_pattern);
		if (pattern == null)
			pattern = "*";
		pattern = pattern.replace('*', '%');
		Collection<RawSchema> result = new ArrayList<RawSchema>();
		try {
			select_schema_by_pattern.setString(1, pattern);
			ResultSet rs = select_schema_by_pattern.executeQuery();
			while(rs.next()) {
				result.add(new RawSchema(rs.getString(3), rs.getInt(1), rs.getInt(2)));
			}
			return result;
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E30105, pattern);
		} finally {
			select_schema_by_pattern = close(select_schema_by_pattern);
		}
	}
	
	private PreparedStatement select_schema_by_id;
	private static final String SELECT_SCHEMA_BY_ID = 
		"select id, parent, label from " + DB.SCHEMA_NAME + " where id = ?";
	/**
	 * Find a raw schema corresponding to a surrogate.
	 * 
	 * @param surrogate a surrogate
	 * @return a raw schema or null
	 * @throws T2DBException
	 */
	protected RawSchema getRawSchema(Surrogate surrogate) throws T2DBException {
		try {
			select_schema_by_id = open(SELECT_SCHEMA_BY_ID, surrogate, select_schema_by_id);
			select_schema_by_id.setInt(1, getId(surrogate));
			ResultSet rs = select_schema_by_id.executeQuery();
			if (rs.next())
				return new RawSchema(rs.getString(3), rs.getInt(1), rs.getInt(2));
			else
				return null;
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E30104, surrogate.toString());
		} finally {
			select_schema_by_id = close(select_schema_by_id);
		}
	}
	
	private PreparedStatement select_schema_components;
	private static final String SELECT_SCHEMA_COMPONENTS = 
		"select ssn, attrib, prop, value, descrip from " + DB.SCHEMA_ITEM + 
		" where id = ? order by ssn, attrib";
	private RawSchemaComponents getRawSchemaComponents(Surrogate surrogate) throws T2DBException {
		Collection<RawSchemaComponent> result = new ArrayList<RawSchemaComponent>();
		try {
			select_schema_components = open(SELECT_SCHEMA_COMPONENTS, surrogate, select_schema_components);
			select_schema_components.setInt(1, getId(surrogate));
			ResultSet rs = select_schema_components.executeQuery();
			while(rs.next()) {
				result.add(new RawSchemaComponent(rs.getInt(1), rs.getInt(2), rs.getInt(3), 
						rs.getString(4), rs.getString(5)));
			}
			return getRawSchemaComponents(result);
		} catch (Exception e) {
			throw T2DBMsg.exception(e, E.E30114, surrogate.toString());
		} finally {
			select_schema_components = close(select_schema_components);
		}
	}
	
	private RawSchemaComponents getRawSchemaComponents(Collection<RawSchemaComponent> list) throws T2DBException {
		RawSchemaComponents rscs = null;
		if (list.size() != 0) {
			rscs = new RawSchemaComponents();
			for (RawSchemaComponent rsc : list) {
				if (rsc.seriesNr == 0) {
					rscs.attributes.add(rsc);
				} else {
					List<RawSchemaComponent> series = rscs.getSeries(rsc.seriesNr);
					series.add(rsc);
				}
			}
		}
		return rscs;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Collection<AttributeDefinition<?>> makeAttributeDefinitions(Database database, Collection<RawSchemaComponent> allAttr) throws T2DBException {
		Collection<AttributeDefinition<?>> defs = new ArrayList<AttributeDefinition<?>>();
		for (RawSchemaComponent oneAttr : allAttr) {
			AttributeDefinitionImpl<?> def = null;
			if (oneAttr.propId < 1) {
				def = new AttributeDefinitionImpl(oneAttr.seriesNr, oneAttr.attribNr, null, null);
				def.edit();
				def.setErasing(true);
			} else {
				Property<?> property = ((JDBCDatabase)database).getReadMethodsForProperty()
					.getProperty(makeSurrogate(database, DBObjectType.PROPERTY, oneAttr.propId));
				def = new AttributeDefinitionImpl(oneAttr.seriesNr, oneAttr.attribNr, property, property.getValueType().scan(oneAttr.value));
			}
			defs.add(def);
		}
		return defs;
	}

	private RawSchemaComponent find(List<RawSchemaComponent> list, int index) {
		RawSchemaComponent found = null;
		for (RawSchemaComponent rsc : list) {
			if (rsc.attribNr == index) {
				found = rsc;
				break;
			}
		}
		return found;
	}
	
	private Collection<SeriesDefinition> makeSeriesSchemas(Database database, List<List<RawSchemaComponent>> allSeries) throws T2DBException {
		Collection<SeriesDefinition> schemas = new ArrayList<SeriesDefinition>();
		for (List<RawSchemaComponent> oneSeries : allSeries) {
			SeriesDefinitionImpl sch = null;
			String description = null;
			RawSchemaComponent rsc = find(oneSeries, DatabaseBackend.MAGIC_NAME_NR);
			if (rsc != null) {
				if (rsc.propId < 1) {
					sch = new SeriesDefinitionImpl(rsc.seriesNr, null, null);
					sch.edit();
					sch.setErasing(true);
				} else
					description = rsc.description;
			}
			if (sch == null)
				sch = new SeriesDefinitionImpl(oneSeries.get(0).seriesNr, description, makeAttributeDefinitions(database, oneSeries));
			schemas.add(sch);
		}
		return schemas;
	}

	/**
	 * Make an UpdatableSchema from a RawSchema. Cycles are detected but do not
	 * result in an exception being thrown. Schemas with a cycle cannot be
	 * resolved but can still be updated so that the problem can be fixed
	 * without resorting to low level tools.
	 * 
	 * @param database
	 * @param rawSchema
	 * @param cycleDetector
	 * @return an updatable schema
	 * @throws T2DBException
	 */
	private UpdatableSchema getSchema(Database database, RawSchema rawSchema, Set<Integer> cycleDetector) throws T2DBException {
		
		UpdatableSchema schema = null;
		if (cycleDetector == null)
			cycleDetector = new HashSet<Integer>();

		boolean cycleDetected = !cycleDetector.add(rawSchema.getId());
		
		String name = rawSchema.getName();
		UpdatableSchema base = null;
		if (rawSchema.getParent() > 0 && !cycleDetected) {
			Surrogate parentKey = makeSurrogate(database, DBObjectType.SCHEMA, rawSchema.getParent());
			RawSchema rawBaseSchema = getRawSchema(parentKey);
			if (rawBaseSchema == null)
				throw T2DBMsg.exception(E.E30116, rawSchema.getParent(), name);
			base = getSchema(database, rawBaseSchema, cycleDetector);
		}
		Surrogate surrogate = makeSurrogate(database, DBObjectType.SCHEMA, rawSchema.getId());
		RawSchemaComponents rawComponents = getRawSchemaComponents(surrogate);
		Collection<AttributeDefinition<?>> attributeDefs = null;
		Collection<SeriesDefinition> seriesDefinitions = null;
		if (rawComponents != null) {
			attributeDefs = makeAttributeDefinitions(database, rawComponents.attributes);
			seriesDefinitions = makeSeriesSchemas(database, rawComponents.series);
		}
		schema = new UpdatableSchemaImpl(name, base, attributeDefs, seriesDefinitions, surrogate);
		return schema;
	}

}