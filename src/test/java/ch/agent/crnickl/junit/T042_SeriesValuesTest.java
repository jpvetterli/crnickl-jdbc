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

import java.util.Collection;

import ch.agent.crnickl.T2DBMsg.D;
import ch.agent.crnickl.api.Chronicle;
import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Schema;
import ch.agent.crnickl.api.Series;
import ch.agent.crnickl.api.UpdatableChronicle;
import ch.agent.crnickl.api.UpdatableSchema;
import ch.agent.crnickl.api.UpdatableSeries;
import ch.agent.t2.T2Msg.K;
import ch.agent.t2.time.Adjustment;
import ch.agent.t2.time.Day;
import ch.agent.t2.time.Range;
import ch.agent.t2.time.TimeDomain;
import ch.agent.t2.time.TimeIndex;
import ch.agent.t2.timeseries.Observation;
import ch.agent.t2.timeseries.TimeAddressable;
import ch.agent.t2.timeseries.TimeIndexable;
import ch.agent.t2.timeseries.TimeSeriesFactory;

public class T042_SeriesValuesTest extends AbstractTest {

	private static Database db;
	protected static String SCHEMA = "t042";
	private static final String CHRONICLE = "bt." + SCHEMA;
	private static final String SERIES = CHRONICLE+ ".test";
	private static final String SERIES_TYPE = "numeric";
	private static final TimeDomain SERIES_DOMAIN = Day.DOMAIN;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		emptySeries(db);
	}

	@Override
	protected void firstSetUp() throws Exception {
		db = DBSetUp.getDatabase();
		createSchema(db);
		createChronicle(db);
		db.commit();
	}

	@Override
	protected void lastTearDown() throws Exception {
		Util.deleteChronicles(db, CHRONICLE);
		Util.deleteSchema(db, SCHEMA);
	}

	public void testSparsity() {
		try {
			Series<Double> s = db.getUpdatableSeries(SERIES, true);
			assertEquals(isSparse(), s.isSparse());
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	public void testEmptyRange() {
		try {
			UpdatableSeries<Double> s = db.getUpdatableSeries(SERIES, true);
			assertEquals(new Range(s.getTimeDomain()), s.getRange());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testNonEmptyRange() {
		try {
			TimeAddressable<Double> ts = makeTimeSeries();
			assertEquals(62, ts.getRange().getSize());
			UpdatableSeries<Double> s = db.getUpdatableSeries(SERIES, true);
			s.setValues(ts);
			// don't use makeSeries() here
			assertEquals(ts.getRange(), s.getRange());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testNonEmptyRangeInDatabase() {
		try {
			TimeAddressable<Double> ts = makeTimeSeries();
			Series<Double> s = makeSeries(ts);
			Series<Double> s2 = db.getSeries(SERIES, true);
			assertNotSame(s, s2);
			assertEquals(ts.getRange(), s2.getRange());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testSubRangeHasNoMissingValuesAtTheBoundaries() {
		try {
			Series<Double> s = makeSeries(makeTimeSeries());
			TimeDomain dom = s.getTimeDomain();
			TimeAddressable<Double> ts = s.getValues(new Range(dom, "2011-05-15", "2011-07-15", Adjustment.NONE));
			assertEquals(new Range(dom, "2011-06-01", "2011-07-01", Adjustment.NONE), ts.getRange());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetValueOutOfRange() {
		try {
			Series<Double> s = makeSeries(makeTimeSeries());
			TimeIndex t = s.getTimeDomain().time("2011-07-15");
			assertEquals(Double.NaN, s.getValue(t));
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetMissingValue_1() {
		try {
			Series<Double> s = makeSeries(makeTimeSeries());
			TimeIndex t = s.getTimeDomain().time("2011-06-15");
			assertEquals(Double.NaN, s.getValue(t));
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetMissingValue_2() {
		try {
			Series<Double> s = makeSeries(makeTimeSeries());
			TimeIndex t = s.getTimeDomain().time("2011-06-15");
			assertEquals(Double.NaN, s.getValue(t));
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetValue() {
		try {
			Series<Double> s = makeSeries(makeTimeSeries());
			TimeIndex t = s.getTimeDomain().time("2011-07-01");
			assertEquals(201107.01, s.getValue(t));
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetLastValue_1() {
		try {
			Series<Double> s = makeSeries(makeTimeSeries());
			TimeDomain dom = s.getTimeDomain();
			Range range = new Range(dom.time("2011-05-15"), dom.time("2011-07-15"));
			TimeAddressable<Double> ts = s.getValues(range);
			Observation<Double> obs = ts.getLast(dom.time("2011-06-01"));
			assertEquals("2011-06-01", obs.getTime().toString());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetLastValue_2() {
		try {
			Series<Double> s = makeSeries(makeTimeSeries());
			TimeIndex t = s.getTimeDomain().time("2011-04-30");
			Observation<Double> obs = s.getLastObservation(t);
			assertNull(obs);
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetLastValue_3() {
		try {
			Series<Double> s = makeSeries(makeTimeSeries());
			TimeDomain dom = s.getTimeDomain();
			Range range = new Range(dom.time("2011-05-15"), dom.time("2011-07-15"));
			TimeAddressable<Double> ts = s.getValues(range);
			Observation<Double> obs = ts.getLast(dom.time("2011-05-31"));
			assertNull(obs);
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetFirstValue_1() {
		try {
			Series<Double> s = makeSeries(makeTimeSeries());
			TimeIndex t = s.getTimeDomain().time("2011-06-01");
			assertEquals(201106.01, s.getFirstObservation(t).getValue());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetFirstValue_2() {
		try {
			Series<Double> s = makeSeries(makeTimeSeries());
			TimeIndex t = s.getTimeDomain().time("2011-05-31");
			assertEquals(201106.01, s.getFirstObservation(t).getValue());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetFirstValue_3() {
		try {
			Series<Double> s = makeSeries(makeTimeSeries());
			TimeIndex t = s.getTimeDomain().time("2011-07-31");
			assertNull(s.getFirstObservation(t));
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	public void testMoreSeries() {
		try {
			UpdatableSeries<Double> s = makeSeries(makeTimeSeries2()).edit();
			TimeAddressable<Double> ts = s.getValues(null);
			Range range = new Range(s.getTimeDomain(), "2011-05-01", "2011-07-03", Adjustment.NONE);
			assertEquals(64, range.getSize());
			assertEquals(range, ts.getRange());
			assertEquals(range, s.getRange());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetLastValue_4() {
		try {
			Series<Double> s = makeSeries(makeTimeSeries2());
			TimeIndex t = s.getTimeDomain().time("2011-06-04");
			assertEquals(201106.02,  s.getLastObservation(t).getValue());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetLastValue_5() {
		try {
			Series<Double> s = makeSeries(makeTimeSeries2());
			TimeIndex t = s.getTimeDomain().time("2011-05-31");
			assertEquals(201105.31,  s.getLastObservation(t).getValue());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetFirstValue_4() {
		try {
			Series<Double> s = makeSeries(makeTimeSeries2());
			TimeIndex t = s.getTimeDomain().time("2011-06-01");
			assertEquals(201106.01,  s.getFirstObservation(t).getValue());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetFirstValue_5() {
		try {
			Series<Double> s = makeSeries(makeTimeSeries2());
			TimeIndex t = s.getTimeDomain().time("2011-06-03");
			assertEquals(201106.30,  s.getFirstObservation(t).getValue());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetFirstValue_6() {
		try {
			Series<Double> s = makeSeries(makeTimeSeries2());
			assertEquals(201105.01, s.getFirstObservation(null).getValue());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testGetLastValue_6() {
		try {
			Series<Double> s = makeSeries(makeTimeSeries2());
			assertEquals(201107.03, s.getLastObservation(null).getValue());
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testSmallGap_1() {
		try {
			Series<Double> s = makeSeriesWithGap("2011-05-01", "2011-05-03");
			// force non-sparse time series
			TimeIndexable<Double> ts = s.getValues(null).asIndexable();
			int missingValuesCount = 0;
			for(Observation<Double> obs : ts) {
				if(ts.isMissing(obs.getValue()))
					missingValuesCount++;
			}
			assertEquals(s.getRange().getSize() - 2, missingValuesCount);
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testSmallGap_2() {
		try {
			Series<Double> s = makeSeriesWithGap("2011-05-01", "2011-07-03");
			// force non-sparse time series
			TimeIndexable<Double> ts = s.getValues(null).asIndexable();
			int missingValuesCount = 0;
			for(Observation<Double> obs : ts) {
				if(ts.isMissing(obs.getValue()))
					missingValuesCount++;
			}
			assertEquals(s.getRange().getSize() - 2, missingValuesCount);
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testLargeGap_1() {
		try {
			Series<Double> s = makeSeriesWithGap("2011-05-01", "2012-09-13");
			// force non-sparse time series
			TimeIndexable<Double> ts = s.getValues(null).asIndexable();
			int missingValuesCount = 0;
			for(Observation<Double> obs : ts) {
				if(ts.isMissing(obs.getValue()))
					missingValuesCount++;
			}
			assertEquals(s.getRange().getSize() - 2, missingValuesCount);
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testLargeGap_2() {
		try {
			// first get the maximum allowed allowed gap
			Series<Double> s = makeSeriesWithGap("2011-05-01", "2011-05-02");
			int maxGap = s.getValues(null).asIndexable().getMaxGap();
			TimeIndex date1 = s.getFirstObservation(null).getTime();
			TimeIndex date2 = date1.add(maxGap + 2); // the maximum allowed gap
			
			Series<Double> s2 = makeSeriesWithGap(date1.toString(), date2.toString());
			// force non-sparse time series (should fail because it creates a gap too large by 1)
			s2.getValues(null).asIndexable();
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	
	public void testLargeGap_3() {
		try {
			// first get the maximum allowed allowed gap
			Series<Double> s = makeSeriesWithGap("2011-05-01", "2011-05-02");
			int maxGap = s.getValues(null).asIndexable().getMaxGap();
			TimeIndex date1 = s.getFirstObservation(null).getTime();
			TimeIndex date2 = date1.add(maxGap + 2 + 1); // a gap too large by 1
			
			Series<Double> s2 = makeSeriesWithGap(date1.toString(), date2.toString());
			// force non-sparse time series (should fail because it creates a gap too large by 1)
			s2.getValues(null).asIndexable();
			expectException();
		} catch (Exception e) {
			assertException(e, K.T5019);
		}
	}

	public void testLargeGap_4() {
		try {
			makeSeriesWithGap("2011-05-01", "2100-05-02");
			// note it can live in the database...
			Series<Double> s2 = db.getSeries(SERIES, true).typeCheck(Double.class);
			s2.getValues(null); // gets the series as an indexable by default
			if (!isSparse())
				expectException();
		} catch (Exception e) {
			if (isSparse())
				fail(e.getMessage());
			else
				assertException(e, D.D50121, K.T5019);
		}
	}
	
	public void testLargeGap_5() {
		try {
			Series<Double> s2 = makeSeriesWithGap("2011-05-01", "2100-05-02");
			s2.getValues(null); // gets the series as a non-indexable by default
		} catch (Exception e) {
			fail(e.toString());
		}
	}

	protected boolean isSparse() {
		return false;
	}
	
	private Schema createSchema(Database db) throws Exception {
		Collection<Schema> schemas = db.getSchemas(SCHEMA);
		switch (schemas.size()) {
		case 1:
			return schemas.iterator().next();
		case 0:
			UpdatableSchema s = db.createSchema(SCHEMA, null);
			s.addSeries(1);
			s.setSeriesName(1, db.getNamingPolicy().split(SERIES)[1]);
			s.setSeriesType(1, SERIES_TYPE);
			s.setSeriesTimeDomain(1, SERIES_DOMAIN);
			s.setSeriesSparsity(1, isSparse());
			s.applyUpdates();
			return s.resolve();
		default:
			throw new IllegalArgumentException(SCHEMA + ": " + schemas.size());
		}
	}
	
	private Chronicle createChronicle(Database db) throws Exception {
		Chronicle c = db.getChronicle(CHRONICLE, false);
		if (c == null) {
			String split[] = db.getNamingPolicy().split(CHRONICLE);
			UpdatableChronicle uc = db.getTopChronicle().edit().createChronicle(split[1], false, CHRONICLE + " (test)", null, createSchema(db));
			uc.applyUpdates();
			c = uc;
		}
		return c;
	}
	
	private Series<Double> emptySeries(Database db) throws Exception {
		UpdatableSeries<Double> s = db.getUpdatableSeries(SERIES, true).typeCheck(Double.class);
		if (!s.inConstruction())
			s.setRange(null);
		s.applyUpdates();
		return s;
	}
	
	private TimeAddressable<Double> makeTimeSeries() throws Exception {
		TimeDomain domain = SERIES_DOMAIN;
		TimeAddressable<Double> ts = TimeSeriesFactory.make(domain, Double.class);
		ts.put(domain.time("2011-05-01"), 201105.01);
		ts.put(domain.time("2011-06-01"), 201106.01);
		ts.put(domain.time("2011-07-01"), 201107.01);
		return ts;
	}
	private TimeAddressable<Double> makeTimeSeries2() throws Exception {
		TimeDomain domain = SERIES_DOMAIN;
		TimeAddressable<Double> ts = TimeSeriesFactory.make(domain, Double.class);
		ts.put(domain.time("2011-05-01"), 201105.01);
		ts.put(domain.time("2011-05-02"), 201105.02);
		ts.put(domain.time("2011-05-03"), 201105.03);
		ts.put(domain.time("2011-05-30"), 201105.30);
		ts.put(domain.time("2011-05-31"), 201105.31);
		ts.put(domain.time("2011-06-01"), 201106.01);
		ts.put(domain.time("2011-06-02"), 201106.02);
		ts.put(domain.time("2011-06-30"), 201106.30);
		ts.put(domain.time("2011-07-01"), 201107.01);
		ts.put(domain.time("2011-07-02"), 201107.02);
		ts.put(domain.time("2011-07-03"), 201107.03);
		return ts;
	}
	
	private Series<Double> makeSeries(TimeAddressable<Double> ts) throws Exception {
		UpdatableSeries<Double> s = db.getUpdatableSeries(SERIES, true);
		s.setValues(ts);
		s.applyUpdates();
		return s;
	}
	
	private Series<Double> makeSeriesWithGap(String date1, String date2) throws Exception {
		UpdatableSeries<Double> s = db.getUpdatableSeries(SERIES, true);
		TimeDomain domain = s.getTimeDomain();
		s.setValue(domain.time(date1), 1d);
		s.setValue(domain.time(date2), 2d);
		s.applyUpdates();
		return s;
	}
	

	
}