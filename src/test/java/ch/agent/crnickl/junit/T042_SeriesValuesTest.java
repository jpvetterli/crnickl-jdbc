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
 * Type: T042_SeriesValuesTest
 * Version: 1.0.1
 */
package ch.agent.crnickl.junit;

import junit.framework.TestCase;
import ch.agent.core.KeyedException;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Series;
import ch.agent.crnickl.api.UpdatableChronicle;
import ch.agent.crnickl.api.UpdatableSchema;
import ch.agent.crnickl.api.UpdatableSeries;
import ch.agent.crnickl.api.UpdatableValueType;
import ch.agent.crnickl.api.ValueType;
import ch.agent.crnickl.jdbc.T2DBJMsg.J;
import ch.agent.t2.T2Msg.K;
import ch.agent.t2.time.Day;
import ch.agent.t2.time.Range;
import ch.agent.t2.time.TimeDomain;
import ch.agent.t2.timeseries.Observation;
import ch.agent.t2.timeseries.TimeAddressable;
import ch.agent.t2.timeseries.TimeSeriesFactory;

public class T042_SeriesValuesTest extends TestCase {

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
			ts.put(dom.time("2011-06-01"),  201106.01);
			ts.put(dom.time("2011-07-01"),  201107.01);
			assertEquals("[2011-05-01, 2011-07-01]", ts.getRange().toString());
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
			assertEquals("[2011-05-01, 2011-07-01]", s.getRange().toString());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetSeries02() {
		try {
			Series<Double> s = db.getSeries(SERIES, true).typeCheck(Double.class);
			TimeDomain dom = Day.DOMAIN;
			Range range = new Range(dom.time("2011-05-15"), dom.time("2011-07-15"));
			TimeAddressable<Double> ts = s.getValues(range);
			assertEquals("[2011-06-01, 2011-07-01]", ts.getRange().toString());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetValue01() {
		try {
			Series<Double> s = db.getSeries(SERIES, true).typeCheck(Double.class);
			TimeDomain dom = Day.DOMAIN;
			Range range = new Range(dom.time("2011-05-15"), dom.time("2011-07-15"));
			TimeAddressable<Double> ts = s.getValues(range);
			Observation<Double> obs = ts.getLast(dom.time("2011-06-01"));
			assertEquals("2011-06-01", obs.getTime().toString());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetValue02() {
		try {
			Series<Double> s = db.getSeries(SERIES, true).typeCheck(Double.class);
			TimeDomain dom = Day.DOMAIN;
			Range range = new Range(dom.time("2011-05-15"), dom.time("2011-07-15"));
			TimeAddressable<Double> ts = s.getValues(range);
			Observation<Double> obs = ts.getLast(dom.time("2011-05-31"));
			assertNull(obs);
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetValue03() {
		try {
			Series<Double> s = db.getSeries(SERIES, true).typeCheck(Double.class);
			TimeDomain dom = Day.DOMAIN;
			Range range = new Range(dom.time("2011-05-15"), dom.time("2011-07-15"));
			TimeAddressable<Double> ts = s.getValues(range);
			Observation<Double> obs = ts.getFirst(dom.time("2011-06-01"));
			assertEquals("2011-06-01", obs.getTime().toString());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetValue04() {
		try {
			Series<Double> s = db.getSeries(SERIES, true).typeCheck(Double.class);
			TimeDomain dom = Day.DOMAIN;
			Range range = new Range(dom.time("2011-05-15"), dom.time("2011-07-15"));
			TimeAddressable<Double> ts = s.getValues(range);
			Observation<Double> obs = ts.getFirst(dom.time("2011-06-02"));
			assertEquals("2011-07-01", obs.getTime().toString());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	public void testGetValue11() {
		try {
			Series<Double> s = db.getSeries(SERIES, true).typeCheck(Double.class);
			TimeDomain dom = Day.DOMAIN;
			Observation<Double> obs = s.getLastObservation(dom.time("2011-06-01"));
			assertNotNull(obs);
			assertEquals("2011-06-01", obs.getTime().toString());
			assertEquals(201106.01, obs.getValue());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetValue12() {
		try {
			Series<Double> s = db.getSeries(SERIES, true).typeCheck(Double.class);
			TimeDomain dom = Day.DOMAIN;
			Observation<Double> obs = s.getLastObservation(dom.time("2011-05-31"));
			assertEquals("2011-05-01", obs.getTime().toString());
			assertEquals(201105.01, obs.getValue());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetValue13() {
		try {
			Series<Double> s = db.getSeries(SERIES, true).typeCheck(Double.class);
			TimeDomain dom = Day.DOMAIN;
			Observation<Double> obs = s.getFirstObservation(dom.time("2011-06-01"));
			assertNotNull(obs);
			assertEquals("2011-06-01", obs.getTime().toString());
			assertEquals(201106.01, obs.getValue());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetValue14() {
		try {
			Series<Double> s = db.getSeries(SERIES, true).typeCheck(Double.class);
			TimeDomain dom = Day.DOMAIN;
			Observation<Double> obs = s.getFirstObservation(dom.time("2011-06-02"));
			assertNotNull(obs);
			assertEquals("2011-07-01", obs.getTime().toString());
			assertEquals(201107.01, obs.getValue());
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	public void testMoreSeries() {
		try {
			TimeDomain dom = Day.DOMAIN;
			UpdatableSeries<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			TimeAddressable<Double> ts = s.getValues(null);
			ts.put(dom.time("2011-05-02"),  -201105.02);
			ts.put(dom.time("2011-05-03"),  201105.03);
			ts.put(dom.time("2011-05-30"),  -201105.30);
			ts.put(dom.time("2011-05-31"),  2011.0531);
			ts.put(dom.time("2011-06-02"),  -201106.02);
			ts.put(dom.time("2011-06-30"),  2011.0630);
			ts.put(dom.time("2011-07-02"),  2011.0702);
			ts.put(dom.time("2011-07-03"),  -2011.0703);
			assertEquals("[2011-05-01, 2011-07-03]", ts.getRange().toString());
			s.setValues(ts);
			s.applyUpdates();
			assertEquals("[2011-05-01, 2011-07-03]", s.getRange().toString());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetValue21() {
		try {
			Series<Double> s = db.getSeries(SERIES, true).typeCheck(Double.class);
			TimeDomain dom = Day.DOMAIN;
			Observation<Double> obs = s.getLastObservation(dom.time("2011-06-04"));
			assertNotNull(obs);
			assertEquals("2011-06-02", obs.getTime().toString());
			assertEquals(-201106.02, obs.getValue());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetValue22() {
		try {
			Series<Double> s = db.getSeries(SERIES, true).typeCheck(Double.class);
			TimeDomain dom = Day.DOMAIN;
			Observation<Double> obs = s.getLastObservation(dom.time("2011-05-31"));
			assertEquals("2011-05-31", obs.getTime().toString());
			assertEquals(2011.0531, obs.getValue());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetValue23() {
		try {
			Series<Double> s = db.getSeries(SERIES, true).typeCheck(Double.class);
			TimeDomain dom = Day.DOMAIN;
			Observation<Double> obs = s.getFirstObservation(dom.time("2011-06-01"));
			assertNotNull(obs);
			assertEquals("2011-06-01", obs.getTime().toString());
			assertEquals(201106.01, obs.getValue());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetValue24() {
		try {
			Series<Double> s = db.getSeries(SERIES, true).typeCheck(Double.class);
			TimeDomain dom = Day.DOMAIN;
			Observation<Double> obs = s.getFirstObservation(dom.time("2011-06-03"));
			assertNotNull(obs);
			assertEquals("2011-06-30", obs.getTime().toString());
			assertEquals(2011.0630, obs.getValue());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetValue25() {
		try {
			Series<Double> s = db.getSeries(SERIES, true).typeCheck(Double.class);
			TimeDomain dom = Day.DOMAIN;
			Observation<Double> obs = s.getFirstObservation(dom.time("2011-06-03"));
			assertNotNull(obs);
			assertEquals("2011-06-30", obs.getTime().toString());
			obs = s.getFirstObservation(dom.time("2011-05-01"));
			assertEquals("2011-05-01", obs.getTime().toString());
			obs = s.getLastObservation(dom.time("2011-07-03"));
			assertEquals("2011-07-03", obs.getTime().toString());
			obs = s.getFirstObservation(null);
			assertEquals("2011-05-01", obs.getTime().toString());
			obs = s.getLastObservation(null);
			assertEquals("2011-07-03", obs.getTime().toString());
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	public void testCreateGap() {
		try {
			TimeDomain dom = Day.DOMAIN;
			UpdatableSeries<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			TimeAddressable<Double> ts = s.getValues(null);
			ts.put(dom.time("2012-05-02"),  201205.02);
			assertEquals("[2011-05-01, 2012-05-02]", ts.getRange().toString());
			s.setValues(ts);
			s.applyUpdates();
			assertEquals("[2011-05-01, 2012-05-02]", s.getRange().toString());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testCreateGapAndCount() {
		try {
			Series<Double> s = db.getSeries(SERIES, true).typeCheck(Double.class);
			assertEquals("[2011-05-01, 2012-05-02]", s.getRange().toString());
			TimeAddressable<Double> ts = s.getValues(null);
			int count = 0;
			for (Observation<Double> obs : ts) {
				if (ts.isMissing(obs.getValue()))
					count++;
			}
			assertTrue(count > 100);
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	public void testCreateLargeGap() {
		try {
			TimeDomain dom = Day.DOMAIN;
			UpdatableSeries<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			TimeAddressable<Double> ts = s.getValues(null);
			ts.put(dom.time("2022-05-02"), 202205.02);
			s.setValues(ts);
			s.applyUpdates();
			Series<Double> s2 = db.getSeries(SERIES, true).typeCheck(Double.class);
			s2.getValues(null);
			fail("exception expected");
		} catch (KeyedException e) {
			assertEquals(J.J50121, e.getMsg().getKey());
			assertEquals(K.T5019, ((KeyedException) e.getCause()).getMsg().getKey());
		}
	}
	
	public void testCreateLargeGap2() {
		try {
			TimeDomain dom = Day.DOMAIN;
			UpdatableSeries<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			s.setValue(dom.time("2022-05-02"), 202205.02);
			Observation<Double> obs = s.getFirstObservation(dom.time("2012-12-03"));
			assertNotNull(obs);
			assertEquals("2022-05-02", obs.getTime().toString());
			assertEquals(202205.02, obs.getValue());
			s.applyUpdates();
			Series<Double> s2 = db.getSeries(SERIES, true).typeCheck(Double.class);
			s2.getValues(null);
		} catch (KeyedException e) {
			assertEquals(J.J50121, e.getMsg().getKey());
			assertEquals(K.T5019, ((KeyedException) e.getCause()).getMsg().getKey());
		}
	}
	
}