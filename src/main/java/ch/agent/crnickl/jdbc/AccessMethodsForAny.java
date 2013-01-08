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

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.api.Series;
import ch.agent.crnickl.api.UpdatableSeries;
import ch.agent.crnickl.api.ValueType;
import ch.agent.crnickl.impl.ChronicleUpdatePolicy;
import ch.agent.crnickl.impl.ValueAccessMethods;
import ch.agent.t2.time.Range;
import ch.agent.t2.time.TimeIndex;
import ch.agent.t2.timeseries.Observation;
import ch.agent.t2.timeseries.TimeAddressable;

/**
 * An implementation of {@link ValueAccessMethods} which
 * stores values as strings. It supports  
 * any type supported by a {@link ValueType}.
 * <p>
 * <em>Methods are only stubs. Actually using them throws exceptions.</em>
 *
 * @author Jean-Paul Vetterli
 * @param <T> the data type of values
 */
public class AccessMethodsForAny<T> extends JDBCDatabaseMethods implements ValueAccessMethods<T> {

	private ValueType<T> valueType;
	
	/**
	 * Construct an access method object.
	 */
	public AccessMethodsForAny() {
	}
	
	/**
	 * Set the value type.
	 * 
	 * @param valueType a value type
	 */
	public void setValueType(ValueType<T> valueType) {
		this.valueType = valueType;
	}
	
	private RuntimeException ouch() {
		return new RuntimeException("ACCESS METHODS NOT IMPLEMENTED: " + valueType.getName());
	}
	
	@Override
	public Range getRange(Series<T> series) throws T2DBException {
		throw ouch();
	}

	@Override
	public long getValues(Series<T> series, Range range, TimeAddressable<T> ts) throws T2DBException {
		throw ouch();
	}

	@Override
	public Observation<T> getFirst(Series<T> series, TimeIndex time) throws T2DBException {
		throw ouch();
	}

	@Override
	public Observation<T> getLast(Series<T> series, TimeIndex time) throws T2DBException {
		throw ouch();
	}

	@Override
	public boolean deleteValue(UpdatableSeries<T> series, TimeIndex t, ChronicleUpdatePolicy policy) throws T2DBException {
		throw ouch();
	}

	@Override
	public boolean updateSeries(UpdatableSeries<T> series, Range range,	ChronicleUpdatePolicy policy) throws T2DBException {
		throw ouch();
	}

	@Override
	public long updateValues(UpdatableSeries<T> series,	TimeAddressable<T> values, ChronicleUpdatePolicy policy) throws T2DBException {
		throw ouch();
	}

}
