package ch.agent.crnickl.junit;

import junit.framework.TestCase;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Series;
import ch.agent.crnickl.api.UpdatableChronicle;
import ch.agent.crnickl.api.UpdatableSchema;
import ch.agent.crnickl.api.UpdatableSeries;
import ch.agent.crnickl.api.UpdatableValueType;
import ch.agent.crnickl.api.ValueType;
import ch.agent.t2.time.Day;
import ch.agent.t2.time.Range;
import ch.agent.t2.time.TimeDomain;
import ch.agent.t2.timeseries.TimeAddressable;
import ch.agent.t2.timeseries.TimeSeriesFactory;

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
 * Package: ch.agent.crnickl.junit
 * Type: T041_UpdatableSeriesTest
 * Version: 1.0.0
 */
/**
 * T041_UpdatableSeriesTest is a test written when upgrading 2 UpdatableSeries 
 * methods: getFirstObservation and getLastObservation. 
 *
 * @author Jean-Paul Vetterli
 * @version 1.0.0
 */
public class T041_UpdatableSeriesTest extends TestCase {

	private Database db;
	private static boolean clean;
	private static final String SERIES = "bt.test040.test";

	@Override
	protected void setUp() throws Exception {
		db = DBSetUp.getDatabase();
		if (!clean) {
	
			// need a "number" value type
			if (db.getValueTypes("number").size() == 0) {
				UpdatableValueType<String> numberType = db.createValueType("number", false, "NUMBER");
				numberType.applyUpdates();
				@SuppressWarnings("rawtypes")
				UpdatableValueType<ValueType> typeType = db.getTypeBuiltInProperty().getValueType().typeCheck(ValueType.class).edit();
				typeType.addValue(numberType, null);
				typeType.applyUpdates();
			}
			
			// need a schema
			UpdatableSchema schema = null;
			if (db.getSchemas("t040").size() == 0) {
				schema = db.createSchema("t040", null);
				schema.addSeries(1);
				schema.setSeriesName(1, "test");
				schema.setSeriesType(1, "number");
				schema.setSeriesTimeDomain(1, Day.DOMAIN);
				schema.applyUpdates();
			}
			
			// need an entity
			if (db.getChronicle("bt.test040", false) == null) {
				UpdatableChronicle ent = db.getTopChronicle().edit().createChronicle("test040", false, "test entity", null, schema);
				ent.applyUpdates();
			}
			
			for (String name : new String[] { SERIES }) {
				UpdatableSeries<?> s = db.getUpdatableSeries(name, false);
				if (s != null) {
					s.setRange(null);
					s.applyUpdates();
					s.destroy();
					s.applyUpdates();
				}
			}
			db.commit();
			clean = true;
		}
	}

	public void testCreateSeries() {
		try {
			TimeDomain dom = Day.DOMAIN;
			UpdatableSeries<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			TimeAddressable<Double> ts = TimeSeriesFactory.make(dom, Double.class);
			ts.put(dom.time("2011-05-01"),  201105.01);
			ts.put(dom.time("2011-05-02"),  201105.02);
			ts.put(dom.time("2011-06-01"),  201106.01);
			ts.put(dom.time("2011-06-30"),  201106.30);
			ts.put(dom.time("2011-07-01"),  201107.01);
			s.setValues(ts);
			s.applyUpdates();
			assertEquals("[2011-05-01, 2011-07-01]", s.getRange().toString());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetSeries01() {
		try {
			Series<Double> s = db.getSeries(SERIES, true).typeCheck(Double.class);
			assertEquals(201105.01, s.getFirstObservation(Day.DOMAIN.time("2011-01-15")).getValue());
			assertEquals(201106.01, s.getFirstObservation(Day.DOMAIN.time("2011-05-15")).getValue());
			assertEquals(201106.01, s.getLastObservation(Day.DOMAIN.time("2011-06-15")).getValue());
			assertEquals(201107.01, s.getLastObservation(Day.DOMAIN.time("2011-12-15")).getValue());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetSeries02() {
		try {
			UpdatableSeries<Double> s = db.getSeries(SERIES, true).typeCheck(Double.class).edit();
			assertEquals(201105.01, s.getFirstObservation(Day.DOMAIN.time("2011-01-15")).getValue());
			assertEquals(201106.01, s.getFirstObservation(Day.DOMAIN.time("2011-05-15")).getValue());
			assertEquals(201106.01, s.getLastObservation(Day.DOMAIN.time("2011-06-15")).getValue());
			assertEquals(201107.01, s.getLastObservation(Day.DOMAIN.time("2011-12-15")).getValue());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetSeries03a() {
		try {
			UpdatableSeries<Double> s = db.getSeries(SERIES, true).typeCheck(Double.class).edit();
			s.setValue(Day.DOMAIN.time("2011-03-01"),  201103.01);
			s.setValue(Day.DOMAIN.time("2011-03-02"),  201103.02);
			s.setValue(Day.DOMAIN.time("2011-08-01"),  201108.01);
			s.setValue(Day.DOMAIN.time("2011-08-02"),  201108.02);
			assertEquals("[2011-03-01, 2011-08-02]", s.getValues(null).getRange().toString());
			assertEquals("[2011-03-01, 2011-08-02]", s.getRange().toString());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetSeries03b() {
		try {
			UpdatableSeries<Double> s = db.getSeries(SERIES, true).typeCheck(Double.class).edit();
			s.setValue(Day.DOMAIN.time("2011-03-01"),  201103.01);
			s.setValue(Day.DOMAIN.time("2011-03-02"),  201103.02);
			s.setValue(Day.DOMAIN.time("2011-08-01"),  201108.01);
			s.setValue(Day.DOMAIN.time("2011-08-02"),  201108.02);
			assertEquals(201103.01, s.getFirstObservation(Day.DOMAIN.time("2011-01-15")).getValue());
			assertEquals(201106.01, s.getFirstObservation(Day.DOMAIN.time("2011-05-15")).getValue());
			assertEquals(201106.01, s.getLastObservation(Day.DOMAIN.time("2011-06-15")).getValue());
			assertEquals(201108.02, s.getLastObservation(Day.DOMAIN.time("2011-12-15")).getValue());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	/**
	 * Reduce range with setRange().
	 */
	public void testGetSeries04() {
		try {
			UpdatableSeries<Double> s = db.getSeries(SERIES, true).typeCheck(Double.class).edit();
			s.setRange(new Range(Day.DOMAIN.time("2011-05-02"), Day.DOMAIN.time("2011-06-30")));
			assertEquals("[2011-05-02, 2011-06-30]", s.getRange().toString());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	/**
	 * Reduce range with setValue().
	 */
	public void testGetSeries05() {
		try {
			UpdatableSeries<Double> s = db.getSeries(SERIES, true).typeCheck(Double.class).edit();
			s.setValue(Day.DOMAIN.time("2011-05-01"),  null);
			s.setValue(Day.DOMAIN.time("2011-07-01"),  null);
			assertEquals("[2011-05-02, 2011-06-30]", s.getRange().toString());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	/**
	 * Set values and test that get first and last work as expected.
	 */
	public void testGetSeries06() {
		try {
			UpdatableSeries<Double> s = db.getSeries(SERIES, true).typeCheck(Double.class).edit();
			s.setValue(Day.DOMAIN.time("2011-05-01"),  null);
			s.setValue(Day.DOMAIN.time("2011-07-01"),  null);
			assertEquals(201105.02, s.getFirstObservation(Day.DOMAIN.time("2011-01-15")).getValue());
			assertEquals(201106.01, s.getFirstObservation(Day.DOMAIN.time("2011-05-15")).getValue());
			assertEquals(201106.01, s.getLastObservation(Day.DOMAIN.time("2011-06-15")).getValue());
			assertEquals(201106.30, s.getLastObservation(Day.DOMAIN.time("2011-12-15")).getValue());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetSeries07() {
		try {
			UpdatableSeries<Double> s = db.getSeries(SERIES, true).typeCheck(Double.class).edit();
			s.setValue(Day.DOMAIN.time("2011-03-01"),  201103.01);
			s.setValue(Day.DOMAIN.time("2011-03-02"),  201103.02);
			s.setValue(Day.DOMAIN.time("2011-05-01"),  null);
			s.setValue(Day.DOMAIN.time("2011-07-01"),  null);
			s.setValue(Day.DOMAIN.time("2011-08-01"),  201108.01);
			s.setValue(Day.DOMAIN.time("2011-08-02"),  201108.02);
			assertEquals(201103.01, s.getFirstObservation(Day.DOMAIN.time("2011-01-15")).getValue());
			assertEquals(201105.02, s.getFirstObservation(Day.DOMAIN.time("2011-04-15")).getValue());
			assertEquals(201106.30, s.getLastObservation(Day.DOMAIN.time("2011-07-15")).getValue());
			assertEquals(201108.02, s.getLastObservation(Day.DOMAIN.time("2011-12-15")).getValue());
		} catch (Exception e) {
			fail(e.toString());
		}
	}

}