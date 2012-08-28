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
 * Type: JDBCDatabase
 * Version: 1.1.0
 */
package ch.agent.crnickl.jdbc;

import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.api.Attribute;
import ch.agent.crnickl.api.AttributeDefinition;
import ch.agent.crnickl.api.Chronicle;
import ch.agent.crnickl.api.DBObjectType;
import ch.agent.crnickl.api.DatabaseConfiguration;
import ch.agent.crnickl.api.Property;
import ch.agent.crnickl.api.Series;
import ch.agent.crnickl.api.SeriesDefinition;
import ch.agent.crnickl.api.Surrogate;
import ch.agent.crnickl.api.UpdatableChronicle;
import ch.agent.crnickl.api.UpdatableProperty;
import ch.agent.crnickl.api.UpdatableSchema;
import ch.agent.crnickl.api.UpdatableSeries;
import ch.agent.crnickl.api.UpdatableValueType;
import ch.agent.crnickl.api.UpdateEventOperation;
import ch.agent.crnickl.api.ValueType;
import ch.agent.crnickl.impl.DatabaseBackend;
import ch.agent.crnickl.impl.DatabaseBackendImpl;
import ch.agent.crnickl.impl.UpdatableSchemaImpl;
import ch.agent.crnickl.impl.UpdateEventImpl;
import ch.agent.crnickl.impl.UpdateEventPublisherImpl;
import ch.agent.crnickl.jdbc.T2DBJMsg.J;

/**
 * A JDBC implementation of {@link DatabaseBackendImpl}. 
 * 
 * @author Jean-Paul Vetterli
 * @version 1.1.0
 */
public class JDBCDatabase extends DatabaseBackendImpl {
	
	/*
	 * TODO: investigate pulling interfaces and some for ReadMethodsForFoo and
	 * WriteMethodsForBar into the "impl" layer. Is the separation in read and
	 * write methods still meaningful? Why not simply "access methods"?
	 */
	
	private ReadMethodsForChroniclesAndSeries esRMethods;
	private WriteMethodsForChroniclesAndSeries esWMethods;
	private ReadMethodsForValueType vtRMethods;
	private WriteMethodsForValueType vtWMethods;
	private ReadMethodsForProperty pRMethods;
	private WriteMethodsForProperty pWMethods;
	private ReadMethodsForSchema sRMethods;
	private WriteMethodsForSchema sWMethods;
	private JDBCSession session;
	
	/**
	 * Construct a {@link DatabaseBackend}.
	 * 
	 * @param name the name of the database
	 */
	public JDBCDatabase(String name) {
		super(name);
	}
	
	@Override
	public void configure(DatabaseConfiguration configuration) throws T2DBException {
		new JDBCSession(configuration);
		super.configure(configuration);
		setAccessMethods(ValueType.StandardValueType.NUMBER.name(), new AccessMethodsForNumber());
	}
	
	/*** manage back end store ***/

	/**
	 * Return the JDBC session.
	 * 
	 * @return the session
	 */
	private JDBCSession getJDBCSession() {
		if (session == null)
			session = JDBCSession.getInstance();
		return session;
	}
	
	public Connection getConnection() throws T2DBException {
		return getJDBCSession().getConnection();
	}
	
	@Override
	public void commit() throws T2DBException {
		getJDBCSession().commit();
		((UpdateEventPublisherImpl)getUpdateEventPublisher()).release();
	}

	@Override
	public void rollback() throws T2DBException {
		JDBCSession.rollbackIfAlive();
		int count = ((UpdateEventPublisherImpl)getUpdateEventPublisher()).clear();
		getMessageListener().log(Level.FINER, new T2DBJMsg(J.J00111, count));
	}

	/*** Chronicle and Series ***/
	
	/**
	 * Return the object providing read methods for chronicles and series.
	 * 
	 * @return the object providing read methods for chronicles and series
	 */
	protected ReadMethodsForChroniclesAndSeries getReadMethodsForChronicleAndSeries() {
		if (esRMethods == null)
			esRMethods = new ReadMethodsForChroniclesAndSeries();
		return esRMethods;
	}
	
	/**
	 * Return the object providing write methods for chronicles and series.
	 * 
	 * @return the object providing write methods for chronicles and series
	 */
	protected WriteMethodsForChroniclesAndSeries getWriteMethodsForChroniclesAndSeries() {
		if (esWMethods == null)
			esWMethods = new WriteMethodsForChroniclesAndSeries();
		return esWMethods;
	}
	
	@Override
	public void create(UpdatableChronicle entity) throws T2DBException {
		getWriteMethodsForChroniclesAndSeries().createChronicle(entity);
		publish(new UpdateEventImpl(UpdateEventOperation.CREATE, entity).withComment(entity.getDescription(false)));
	}
	
	@Override
	public void update(UpdatableChronicle entity) throws T2DBException {
		getWriteMethodsForChroniclesAndSeries().updateChronicle(entity, getChronicleUpdatePolicy());
		publish(new UpdateEventImpl(UpdateEventOperation.MODIFY, entity));
	}
	
	@Override
	public void deleteAttributeValue(UpdatableChronicle entity, AttributeDefinition<?> def) throws T2DBException {
		getWriteMethodsForChroniclesAndSeries().deleteAttribute(entity, def);
		publish(new UpdateEventImpl(UpdateEventOperation.MODIFY, entity).withComment("delete attribute #" + def.getNumber()));
	}

	@Override
	public void update(UpdatableChronicle entity, AttributeDefinition<?> def, String value, String description) throws T2DBException {
		getWriteMethodsForChroniclesAndSeries().updateAttribute(entity, def, value, description);
		publish(new UpdateEventImpl(UpdateEventOperation.MODIFY, entity).withComment(String.format("%s=%s", def.getProperty().getName(), value)));
	}

	
	@Override
	public void deleteChronicle(UpdatableChronicle entity) throws T2DBException {
		getWriteMethodsForChroniclesAndSeries().deleteChronicle(entity, getChronicleUpdatePolicy());
		// pass name and description as comment, because they are lost when logger gets them
		String comment = getNamingPolicy().joinValueAndDescription(entity.getName(true), entity.getDescription(false));
		publish(new UpdateEventImpl(UpdateEventOperation.DELETE, entity).withComment(comment));
	}
	
	@Override
	public <T>void create(UpdatableSeries<T> series) throws T2DBException {
		getWriteMethodsForChroniclesAndSeries().createSeries(series);
		publish(new UpdateEventImpl(UpdateEventOperation.CREATE, series));
	}

	@Override
	public <T>void deleteSeries(UpdatableSeries<T> series) throws T2DBException {
		getWriteMethodsForChroniclesAndSeries().deleteSeries(series, getChronicleUpdatePolicy());
		String comment = getNamingPolicy().joinValueAndDescription(series.getName(true), series.getDescription(false));
		publish(new UpdateEventImpl(UpdateEventOperation.DELETE, series).withComment(comment));
	}
	
	@Override
	public Chronicle getChronicle(Chronicle chronicle) throws T2DBException {
		return getReadMethodsForChronicleAndSeries().getChronicle(chronicle.getSurrogate());
	}

	@Override
	public Chronicle getChronicleOrNull(Chronicle parent, String simpleName) throws T2DBException {
		return getReadMethodsForChronicleAndSeries().getChronicleOrNull(parent, simpleName);
	}
	
	@Override
	public Collection<Chronicle> getChroniclesByParent(Chronicle parent) throws T2DBException {
		return getReadMethodsForChronicleAndSeries().getChroniclesByParent(parent);
	}
	
	@Override
	public <T> List<Chronicle> getChroniclesByAttributeValue(Property<T> property, T value, int maxSize) throws T2DBException {
		return getReadMethodsForChronicleAndSeries().getChroniclesByAttributeValue(property, value, maxSize);
	}

	@Override
	public boolean getAttributeValue(List<Chronicle> chronicles, Attribute<?> attribute)	throws T2DBException {
		return getReadMethodsForChronicleAndSeries().getAttributeValue(chronicles, attribute);
	}

	@Override
	public <T> Series<T>[] getSeries(Chronicle chronicle, String[] names, int[] numbers)	throws T2DBException {
		return getReadMethodsForChronicleAndSeries().getSeries(chronicle, names, numbers);
	}
	
	@Override
	public <T>Series<T> getSeries(Surrogate surrogate) throws T2DBException {
		checkSurrogate(surrogate, DBObjectType.SERIES);
		Series<T> series = getReadMethodsForChronicleAndSeries().getSeries(surrogate);
		if (series == null)
			throw T2DBJMsg.exception(J.J50104, surrogate.toString());
		return series;

	}

	/*** Property ***/
	
	/**
	 * Return the object providing read methods for properties.
	 * 
	 * @return the object providing read methods for properties
	 */
	protected ReadMethodsForProperty getReadMethodsForProperty() {
		if (pRMethods == null)
			pRMethods = new ReadMethodsForProperty();
		return pRMethods;
	}

	/**
	 * Return the object providing write methods for properties.
	 * 
	 * @return the object providing write methods for properties
	 */
	protected WriteMethodsForProperty getWriteMethodsForProperty() {
		if (pWMethods == null)
			pWMethods = new WriteMethodsForProperty();
		return pWMethods;
	}
	
	@Override
	public Collection<Property<?>> getProperties(String pattern) throws T2DBException {
		return getReadMethodsForProperty().getProperties(this, pattern);
	}
	
	@Override
	public Property<?> getProperty(Surrogate surrogate) throws T2DBException {
		checkSurrogate(surrogate, DBObjectType.PROPERTY);
		Property<?> vt = getReadMethodsForProperty().getProperty(surrogate);
		if (vt == null)
			throw T2DBJMsg.exception(J.J20109, surrogate.toString());
		return vt;
	}

	@Override
	public Property<?> getProperty(String name) throws T2DBException {
		return getReadMethodsForProperty().getProperty(this, name);
	}

	@Override
	public void create(UpdatableProperty<?> property) throws T2DBException {
		getWriteMethodsForProperty().createProperty(property);
		publish(new UpdateEventImpl(UpdateEventOperation.CREATE, property));
	}

	@Override
	public void deleteProperty(UpdatableProperty<?> property) throws T2DBException {
		getWriteMethodsForProperty().deleteProperty(property);
		String comment = property.getName();
		publish(new UpdateEventImpl(UpdateEventOperation.DELETE, property).withComment(comment));
	}

	@Override
	public void update(UpdatableProperty<?> property) throws T2DBException {
		getWriteMethodsForProperty().updateProperty(property);
		publish(new UpdateEventImpl(UpdateEventOperation.MODIFY, property));
	}
	
	/*** ValueType ***/
	
	/**
	 * Return the object providing read methods for value types.
	 * 
	 * @return the object providing read methods for value types
	 */
	protected ReadMethodsForValueType getReadMethodsForValueType() {
		if (vtRMethods == null)
			vtRMethods = new ReadMethodsForValueType();
		return vtRMethods;
	}

	/**
	 * Return the object providing write methods for value types.
	 * 
	 * @return the object providing write methods for value types
	 */
	protected WriteMethodsForValueType getWriteMethodsForValueType() {
		if (vtWMethods == null)
			vtWMethods = new WriteMethodsForValueType();
		return vtWMethods;
	}

	@Override
	public Collection<ValueType<?>> getValueTypes(String pattern) throws T2DBException {
		return getReadMethodsForValueType().getValueTypes(this, pattern);
	}

	@Override
	public <T>ValueType<T> getValueType(String name) throws T2DBException {
		ValueType<T> vt = getReadMethodsForValueType().getValueType(this, name);
		if (vt == null)
			throw T2DBJMsg.exception(J.J10109, name);
		return vt;
	}

	@Override
	public <T>ValueType<T> getValueType(Surrogate surrogate) throws T2DBException {
		checkSurrogate(surrogate, DBObjectType.VALUE_TYPE);
		ValueType<T> vt = getReadMethodsForValueType().getValueType(surrogate);
		if (vt == null)
			throw T2DBJMsg.exception(J.J10110, surrogate.toString());
		return vt;
	}


	@Override
	public <T>void create(UpdatableValueType<T> valueType) throws T2DBException {
		getWriteMethodsForValueType().createValueType(valueType);
		publish(new UpdateEventImpl(UpdateEventOperation.CREATE, valueType));
	}

	@Override
	public void deleteValueType(UpdatableValueType<?> valueType) throws T2DBException {
		getWriteMethodsForValueType().deleteValueType(valueType);
		String comment = valueType.getName();
		publish(new UpdateEventImpl(UpdateEventOperation.DELETE, valueType).withComment(comment));
	}

	@Override
	public void update(UpdatableValueType<?> valueType) throws T2DBException {
		getWriteMethodsForValueType().updateValueType(valueType);
		publish(new UpdateEventImpl(UpdateEventOperation.MODIFY, valueType));
	}

	@Override
	public <T>void update(UpdatableValueType<T> valueType, Map<T, String> added, Map<T, String> edited, Set<T> deleted) throws T2DBException {
		getWriteMethodsForValueType().updateValueType(valueType, added, edited, deleted);
		publish(new UpdateEventImpl(UpdateEventOperation.MODIFY, valueType));
	}
	
	/*** Schemas ***/

	/**
	 * Return the object providing read methods for schemas.
	 * 
	 * @return the object providing read methods for schemass
	 */
	protected ReadMethodsForSchema getReadMethodsForSchema() {
		if (sRMethods == null)
			sRMethods = new ReadMethodsForSchema();
		return sRMethods;
	}
	
	/**
	 * Return the object providing write methods for schemas.
	 * 
	 * @return the object providing write methods for schemas
	 */
	protected WriteMethodsForSchema getWriteMethodsForSchema() {
		if (sWMethods == null)
			sWMethods = new WriteMethodsForSchema();
		return sWMethods;
	}
	
	@Override
	public Collection<Surrogate> getSchemaSurrogates(String pattern) throws T2DBException {
		return getReadMethodsForSchema().getSchemaSurrogateList(this, pattern);
	}

	@Override
	public UpdatableSchema getUpdatableSchema(Surrogate surrogate) throws T2DBException {
		UpdatableSchema schema = getReadMethodsForSchema().getSchema(surrogate);
		if (schema == null)
			throw T2DBJMsg.exception(J.J30109, surrogate.toString());
		return schema;
	}

	@Override
	public void create(UpdatableSchema schema) throws T2DBException {
		UpdatableSchema base = schema.getBase();
		getWriteMethodsForSchema().createSchema((UpdatableSchemaImpl) schema, base);
		publish(new UpdateEventImpl(UpdateEventOperation.CREATE, schema));
	}

	@Override
	public void create(UpdatableSchema schema, int seriesNr, String description, AttributeDefinition<?> def) throws T2DBException {
		getWriteMethodsForSchema().createSchemaComponent((UpdatableSchemaImpl) schema, seriesNr, description, def);
	}

	@Override
	public boolean update(UpdatableSchema schema, UpdatableSchema base, String name) throws T2DBException {
		return getWriteMethodsForSchema().updateSchema((UpdatableSchemaImpl) schema, base, name);
	}

	@Override
	public void update(UpdatableSchema schema, int seriesNr, String description, AttributeDefinition<?> def) throws T2DBException {
		getWriteMethodsForSchema().updateSchemaComponent(schema, seriesNr, description, def);
	}

	@Override
	public void deleteAttributeInSchema(UpdatableSchema schema, int seriesNr, int attribNr) throws T2DBException {
		getWriteMethodsForSchema().deleteSchemaComponent(schema, seriesNr, attribNr);
	}

	@Override
	public void deleteSeriesInSchema(UpdatableSchema schema, int seriesNr) throws T2DBException {
		getWriteMethodsForSchema().deleteSchemaComponents(schema, seriesNr);
	}

	@Override
	public void deleteSchema(UpdatableSchema schema) throws T2DBException {
		getWriteMethodsForSchema().deleteSchema((UpdatableSchemaImpl) schema, getSchemaUpdatePolicy());
		String comment = schema.getName();
		publish(new UpdateEventImpl(UpdateEventOperation.DELETE, schema).withComment(comment));
	}
	
	@Override
	public Collection<Surrogate> findChronicles(Collection<UpdatableSchema> schemas) throws T2DBException {
		return getWriteMethodsForSchema().findChronicles(schemas);
	}

	@Override
	public Collection<Surrogate> findChronicles(Property<?> property, Collection<UpdatableSchema> schemas) throws T2DBException {
		return getWriteMethodsForSchema().findChronicles(property, schemas);
	}

	@Override
	public Collection<Surrogate> findChronicles(SeriesDefinition ss, Collection<UpdatableSchema> schemas) throws T2DBException {
		return getWriteMethodsForSchema().findChronicles(ss, schemas);
	}

	@Override
	public String toString() {
		return getJDBCSession().toString();
	}

}
