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
 * Type: T040_SeriesTest
 * Version: 1.0.1
 */
package ch.agent.crnickl.junit;

import junit.framework.TestCase;
import ch.agent.core.KeyedException;
import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg.D;
import ch.agent.crnickl.api.Chronicle;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Schema;
import ch.agent.crnickl.api.Series;
import ch.agent.crnickl.api.UpdatableChronicle;
import ch.agent.crnickl.api.UpdatableSchema;
import ch.agent.crnickl.api.UpdatableSeries;
import ch.agent.crnickl.api.UpdatableValueType;
import ch.agent.crnickl.api.ValueType;
import ch.agent.t2.time.Adjustment;
import ch.agent.t2.time.Day;
import ch.agent.t2.time.Range;
import ch.agent.t2.time.TimeIndex;

public class T040_SeriesTest extends TestCase {

	private Database db;
	private static boolean clean;
	private static final String CHRONICLE = "bt.test040";
	private static final String SERIES = CHRONICLE + ".test";
	private static final String SERIES2 = CHRONICLE + ".seriestest.test";

	@Override
	protected void setUp() throws Exception {
		db = DBSetUp.getDatabase();
		if (!clean) {
			DBSetUp.getDatabaseManager().deleteChronicleCollection(CHRONICLE);
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
			
			for (String name : new String[] { SERIES, SERIES2 }) {
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

	public void test1() {
		try {
			Series<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			assertEquals(SERIES, s.getName(true));
			assertTrue(s.getSurrogate().inConstruction());
		} catch (Exception e) {
//			e.printStackTrace();
			fail(e.toString());
		}
	}
	
	public void test2() {
		try {
			UpdatableSeries<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			TimeIndex t = s.getTimeDomain().time("2011-03-08");
			s.scanValue(t, "42");
			assertEquals(42.0, s.getValue(t));
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void test3() {
		try {
			Series<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			TimeIndex t = s.getTimeDomain().time("2011-03-08");
			assertEquals(Double.NaN, s.getValue(t));
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void test4() {
		try {
			UpdatableSeries<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			TimeIndex t = s.getTimeDomain().time("2011-03-08");
			s.scanValue(t, "42");
			s.applyUpdates();
			assertEquals(42.0, s.getValue(t));
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void test5() {
		try {
			Series<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			TimeIndex t = s.getTimeDomain().time("2011-03-08");
			assertEquals(42.0, s.getValue(t));
			assertFalse(s.getSurrogate().inConstruction());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void test6() {
		try {
			db.rollback();
			Series<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			TimeIndex t = s.getTimeDomain().time("2011-03-08");
			assertEquals(Double.NaN, s.getValue(t));
			assertTrue(s.getSurrogate().inConstruction());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	public void test7() {
		try {
			UpdatableSeries<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			assertTrue(s.getSurrogate().inConstruction());
			TimeIndex t = s.getTimeDomain().time("2011-03-08");
			s.scanValue(t, "42");
			assertEquals(42., s.getValue(t));
			s.applyUpdates();
			db.commit();
			assertEquals(42.0, s.getValue(t));
		} catch (Exception e) {
//			e.printStackTrace();
			fail(e.toString());
		}
	}
	
	public void test8() {
		try {
			db.rollback();
			Series<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			TimeIndex t = s.getTimeDomain().time("2011-03-08");
			assertEquals(42.0, s.getValue(t));
			System.out.println(getName() + " XXX " + s.getValues(null));
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void test9() {
		try {
			UpdatableSeries<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			s.setRange(null);
			s.applyUpdates();
			s.destroy();
			s.applyUpdates();
			assertFalse(s.getSurrogate().getObject().isValid());
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	public void test10() {
		try {
			UpdatableSeries<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			TimeIndex t = s.getTimeDomain().time("2011-03-08");
			s.scanValue(t, "42");
			s.applyUpdates();
			((UpdatableChronicle) s.getChronicle()).applyUpdates();
			assertEquals(42.0, s.getValue(t));
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void test11() {
		// an entity does not exist 
		try {
			String[] split = db.getNamingPolicy().split(SERIES2);
			UpdatableChronicle testEntity = ((UpdatableChronicle)db.getTopChronicle()).createChronicle(split[0], false, "RRMProviderTest", null, null);
			testEntity.createSeries(split[1]).typeCheck(Double.class);
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D01104, e.getMsg().getKey());
		}
	}
	
	public void test12() {
		// an entity does not exist 
		try {
			db.getUpdatableSeries(SERIES2, true).typeCheck(Double.class);
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D40102, e.getMsg().getKey());
		}
	}
	
	public void test13() {
		try {
			Series<Double>[] s = db.getChronicle("bt.test040", true).getSeries(new String[]{"test"}, null, true);
			assertEquals(Double.class, s[0].getValueType().getType());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void test14() {
		try {
			Chronicle chronicle = db.getChronicle("bt.test040", true);
			Series<String>[] s = chronicle.getSeries(new String[]{"test"}, String.class, true);
			assertEquals(Double.class, s[0].getValueType().getType());
			fail("exception expected");
		} catch (T2DBException e) {
			assertEquals(D.D50101, e.getMsg().getKey());
		}
	}
	
	public void test15() {
		try {
			Chronicle chronicle = db.getChronicle("bt.test040", true);
			Series<Double>[] s = chronicle.getSeries(new String[]{"test"}, Double.class, true);
			assertEquals(Double.class, s[0].getValueType().getType());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void test20() {
		try {
			Chronicle chronicle = db.getChronicle("bt.test040", true);
			assertEquals(1, chronicle.getSchema(true).getSeriesDefinitions().size());
			Schema s0 = chronicle.getSchema(true);
			UpdatableSchema usch = s0.edit();
			usch.addSeries(4);
			usch.setSeriesName(4, "test4");
			usch.setSeriesType(4, "number");
			usch.setSeriesTimeDomain(4, Day.DOMAIN);
			assertEquals(1, usch.getSeriesDefinitions().size());
			usch.applyUpdates();
			assertEquals(2, usch.getSeriesDefinitions().size());
			db.commit();
			assertEquals(1, chronicle.getSchema(true).getSeriesDefinitions().size());
			chronicle = db.getChronicle("bt.test040", true);
			assertEquals(2, chronicle.getSchema(true).getSeriesDefinitions().size());
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	public void test21() {
		try {
			UpdatableSeries<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			TimeIndex t = s.getTimeDomain().time("2011-03-08");
			s.scanValue(t, "42.01");
			s.applyUpdates();
			db.commit();
			Chronicle chronicle = db.getChronicle("bt.test040", true);
			Series<Double>[] ss = chronicle.getSeries(new String[]{"test", "test4"}, Double.class, true);
			assertEquals(Double.class, ss[0].getValueType().getType());
			assertEquals(42.01, ss[0].getValue(t));
			assertNull(ss[1]);
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void test22() {
		try {
			UpdatableSeries<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			TimeIndex t = s.getTimeDomain().time("2011-03-08");
			s.scanValue(t, "42");
			s.applyUpdates();
			db.commit();
			Chronicle chronicle = db.getChronicle("bt.test040", true);
			chronicle.getSeries(new String[]{"test", "test2", "test4"}, Double.class, true);
			fail("exception exptected");
		} catch (KeyedException e) {
			assertEquals(D.D30121, e.getMsg().getKey());
		}
	}
	
	public void test23() {
		try {
			UpdatableSeries<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			TimeIndex t = s.getTimeDomain().time("2011-03-08");
			s.scanValue(t, "42");
			s.applyUpdates();
			db.commit();
			Chronicle chronicle = db.getChronicle("bt.test040", false);
			Series<Double>[] ss = chronicle.getSeries(new String[]{"test", "test2", "test4"}, Double.class, false);
			assertEquals(Double.class, ss[0].getValueType().getType());
			assertEquals(42.0, ss[0].getValue(t));
			assertNull(ss[1]);
			assertNull(ss[2]);
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	public void test24() {
		try {
			UpdatableSeries<Double> s = db.getSeries(SERIES, true).edit().typeCheck(Double.class);
			TimeIndex t = s.getTimeDomain().time("2011-03-08");
			assertEquals(42.0, s.getValue(t));
			t = s.getTimeDomain().time("2011-03-14");
			s.scanValue(t, "51.3");
			s.scanValue(t.add(2), "55");
			s.applyUpdates();
			System.out.println(t.getTimeDomain() + " " + s.getRange());
			assertEquals(9, s.getRange().getSize());
			System.out.println(s.getValues(null));
			boolean done = s.setRange(new Range(t.getTimeDomain(), "2011-03-12", "2011-03-17", Adjustment.NONE));
			System.out.println(s.getValues(null));
			assertFalse(done);
			done = s.setRange(new Range(t.getTimeDomain(), "2011-03-12", "2011-03-16", Adjustment.NONE));
			s.applyUpdates();
			System.out.println(s.getValues(null));
			assertEquals(3, s.getRange().getSize());
			done = s.setRange(null);
			s.applyUpdates();
			assertEquals(0, s.getRange().getSize());

		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
}