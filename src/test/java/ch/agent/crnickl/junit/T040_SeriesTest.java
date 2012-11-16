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

import ch.agent.crnickl.T2DBMsg.D;
import ch.agent.crnickl.api.Chronicle;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Schema;
import ch.agent.crnickl.api.Series;
import ch.agent.crnickl.api.UpdatableChronicle;
import ch.agent.crnickl.api.UpdatableSchema;
import ch.agent.crnickl.api.UpdatableSeries;
import ch.agent.t2.time.Adjustment;
import ch.agent.t2.time.Day;
import ch.agent.t2.time.Range;
import ch.agent.t2.time.TimeIndex;

public class T040_SeriesTest extends AbstractTest {

	private static Boolean DUMP = false;
	private static Database db;
	private static final String SCHEMA = "t040";
	private static final String CHRONICLE = "bt.t040";
	private static final String SERIES = CHRONICLE + ".test";
	private static final String SERIES2 = CHRONICLE + ".seriestest.test";

	@Override
	protected void firstSetUp() throws Exception {
		db = DBSetUp.getDatabase();
		UpdatableSchema s = db.createSchema(SCHEMA, null);
		s.addSeries(1);
		s.setSeriesName(1, "test");
		s.setSeriesType(1, "numeric");
		s.setSeriesTimeDomain(1, Day.DOMAIN);
		s.applyUpdates();
		String split[] = db.getNamingPolicy().split(CHRONICLE);
		UpdatableChronicle c = db.getTopChronicle().edit()
				.createChronicle(split[1], false, "test entity", null, s.resolve());
		c.applyUpdates();
		db.commit();
	}

	@Override
	protected void lastTearDown() throws Exception {
		Util.deleteChronicles(db, CHRONICLE);
		Util.deleteSchema(db, SCHEMA);
	}
	
	public void test_01() {
		try {
			Series<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			assertEquals(SERIES, s.getName(true));
			assertTrue(s.getSurrogate().inConstruction());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void test_02() {
		try {
			UpdatableSeries<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			TimeIndex t = s.getTimeDomain().time("2011-03-08");
			s.scanValue(t, "42");
			assertEquals(42.0, s.getValue(t));
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void test_03() {
		try {
			Series<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			TimeIndex t = s.getTimeDomain().time("2011-03-08");
			assertEquals(Double.NaN, s.getValue(t));
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void test_04() {
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
	
	public void test_05() {
		try {
			Series<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			TimeIndex t = s.getTimeDomain().time("2011-03-08");
			assertEquals(42.0, s.getValue(t));
			assertFalse(s.getSurrogate().inConstruction());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void test_06() {
		try {
			if (DBSetUp.canRollback()) {
				db.rollback();
				Series<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
				TimeIndex t = s.getTimeDomain().time("2011-03-08");
				assertEquals(Double.NaN, s.getValue(t));
				assertTrue(s.getSurrogate().inConstruction());
			}
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	public void test_07() {
		try {
			UpdatableSeries<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			if (DBSetUp.canRollback())
				assertTrue(s.getSurrogate().inConstruction());
			TimeIndex t = s.getTimeDomain().time("2011-03-08");
			s.scanValue(t, "42");
			assertEquals(42., s.getValue(t));
			s.applyUpdates();
			db.commit();
			assertEquals(42.0, s.getValue(t));
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void test_08() {
		try {
			db.rollback();
			Series<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			TimeIndex t = s.getTimeDomain().time("2011-03-08");
			assertEquals(42.0, s.getValue(t));
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void test_09() {
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

	public void test_10() {
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
	
	public void test_11() {
		// an entity does not exist 
		try {
			String[] split = db.getNamingPolicy().split(SERIES2);
			UpdatableChronicle testEntity = ((UpdatableChronicle)db.getTopChronicle()).createChronicle(split[0], false, "test 2", null, null);
			testEntity.createSeries(split[1]).typeCheck(Double.class);
			expectException();
		} catch (Exception e) {
			assertException(e, D.D01104);
		}
	}
	
	public void test_12() {
		// an entity does not exist 
		try {
			db.getUpdatableSeries(SERIES2, true).typeCheck(Double.class);
			expectException();
		} catch (Exception e) {
			assertException(e, D.D40102);
		}
	}
	
	public void test_13() {
		try {
			Series<Double>[] s = db.getChronicle(CHRONICLE, true).getSeries(new String[]{"test"}, null, true);
			assertEquals(Double.class, s[0].getValueType().getType());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void test_14() {
		try {
			Chronicle chronicle = db.getChronicle(CHRONICLE, true);
			Series<String>[] s = chronicle.getSeries(new String[]{"test"}, String.class, true);
			assertEquals(Double.class, s[0].getValueType().getType());
			expectException();
		} catch (Exception e) {
			assertException(e, D.D50101);
		}
	}
	
	public void test_15() {
		try {
			Chronicle chronicle = db.getChronicle(CHRONICLE, true);
			Series<Double>[] s = chronicle.getSeries(new String[]{"test"}, Double.class, true);
			assertEquals(Double.class, s[0].getValueType().getType());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void test_20() {
		try {
			Chronicle chronicle = db.getChronicle(CHRONICLE, true);
			assertEquals(1, chronicle.getSchema(true).getSeriesDefinitions().size());
			Schema s0 = chronicle.getSchema(true);
			UpdatableSchema usch = s0.edit();
			usch.addSeries(4);
			usch.setSeriesName(4, "test4");
			usch.setSeriesType(4, "numeric");
			usch.setSeriesTimeDomain(4, Day.DOMAIN);
			assertEquals(2, usch.getSeriesDefinitions().size());
			usch.applyUpdates();
			assertEquals(2, usch.getSeriesDefinitions().size());
			db.commit();
			assertEquals(1, chronicle.getSchema(true).getSeriesDefinitions().size());
			chronicle = db.getChronicle(CHRONICLE, true);
			assertEquals(2, chronicle.getSchema(true).getSeriesDefinitions().size());
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	public void test_21() {
		try {
			UpdatableSeries<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			TimeIndex t = s.getTimeDomain().time("2011-03-08");
			s.scanValue(t, "42.01");
			s.applyUpdates();
			db.commit();
			Chronicle chronicle = db.getChronicle(CHRONICLE, true);
			Series<Double>[] ss = chronicle.getSeries(new String[]{"test", "test4"}, Double.class, true);
			assertEquals(2, ss.length);
			assertEquals(Double.class, ss[0].getValueType().getType());
			assertEquals(42.01, ss[0].getValue(t));
			assertNull(ss[1]);
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void test_22() {
		try {
			UpdatableSeries<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			TimeIndex t = s.getTimeDomain().time("2011-03-08");
			s.scanValue(t, "42");
			s.applyUpdates();
			db.commit();
			Chronicle chronicle = db.getChronicle(CHRONICLE, true);
			chronicle.getSeries(new String[]{"test", "test2", "test4"}, Double.class, true);
			expectException();
		} catch (Exception e) {
			assertException(e, D.D30121);
		}
	}
	
	public void test_23() {
		try {
			UpdatableSeries<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
			TimeIndex t = s.getTimeDomain().time("2011-03-08");
			s.scanValue(t, "42");
			s.applyUpdates();
			db.commit();
			Chronicle chronicle = db.getChronicle(CHRONICLE, false);
			Series<Double>[] ss = chronicle.getSeries(new String[]{"test", "test2", "test4"}, Double.class, false);
			assertEquals(Double.class, ss[0].getValueType().getType());
			assertEquals(42.0, ss[0].getValue(t));
			assertNull(ss[1]);
			assertNull(ss[2]);
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	public void test_24() {
		try {
			UpdatableSeries<Double> s = db.getSeries(SERIES, true).edit().typeCheck(Double.class);
			TimeIndex t = s.getTimeDomain().time("2011-03-08");
			assertEquals(42.0, s.getValue(t));
			t = s.getTimeDomain().time("2011-03-14");
			s.scanValue(t, "51.3");
			s.scanValue(t.add(2), "55");
			s.applyUpdates();
			if (DUMP)
				System.out.println(t.getTimeDomain() + " " + s.getRange());
			assertEquals(9, s.getRange().getSize());
			if (DUMP)
				System.out.println(s.getValues(null));
			boolean done = s.setRange(new Range(t.getTimeDomain(), "2011-03-12", "2011-03-17", Adjustment.NONE));
			if (DUMP)
				System.out.println(s.getValues(null));
			assertFalse(done);
			done = s.setRange(new Range(t.getTimeDomain(), "2011-03-12", "2011-03-16", Adjustment.NONE));
			s.applyUpdates();
			if (DUMP)
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