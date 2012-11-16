package ch.agent.crnickl.junit;

import ch.agent.crnickl.api.Database;
import ch.agent.crnickl.api.Series;
import ch.agent.crnickl.api.UpdatableChronicle;
import ch.agent.crnickl.api.UpdatableSchema;
import ch.agent.crnickl.api.UpdatableSeries;
import ch.agent.t2.time.Day;
import ch.agent.t2.time.Range;
import ch.agent.t2.time.TimeDomain;
import ch.agent.t2.timeseries.TimeAddressable;
import ch.agent.t2.timeseries.TimeSeriesFactory;

public class T041_UpdatableSeriesTest extends AbstractTest {

	private static Database db;
	private static final String SERIES = "bt.t040.test";

	@Override
	protected void firstSetUp() throws Exception {
		db = DBSetUp.getDatabase();
		UpdatableSchema s = db.createSchema("t040", null);
		s.addSeries(1);
		s.setSeriesName(1, "test");
		s.setSeriesType(1, "numeric");
		s.setSeriesTimeDomain(1, Day.DOMAIN);
		s.applyUpdates();
		String split[] = db.getNamingPolicy().split("bt.t040");
		UpdatableChronicle c = db.getTopChronicle().edit().createChronicle(split[1], false, "test entity", null, s.resolve());
		c.applyUpdates();
		db.commit();
	}

	@Override
	protected void lastTearDown() throws Exception {
		Util.deleteChronicles(db, "bt.t040");
		Util.deleteSchema(db, "t040");
		db.commit();
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