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
 * Type: AccessMethodsForNumber
 * Version: 1.0.1
 */
package ch.agent.crnickl.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import ch.agent.core.KeyedException;
import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg.D;
import ch.agent.crnickl.api.Series;
import ch.agent.crnickl.api.UpdatableSeries;
import ch.agent.crnickl.impl.ChronicleUpdatePolicy;
import ch.agent.crnickl.impl.Permission;
import ch.agent.crnickl.impl.ValueAccessMethods;
import ch.agent.crnickl.jdbc.T2DBJMsg.J;
import ch.agent.t2.time.Range;
import ch.agent.t2.time.TimeDomain;
import ch.agent.t2.time.TimeIndex;
import ch.agent.t2.timeseries.Observation;
import ch.agent.t2.timeseries.TimeAddressable;

/**
 * An implementation of {@link ValueAccessMethods} for numeric data using {@link java.lang.Double}.
 *
 * @author Jean-Paul Vetterli
 * @version 1.0.1
 */
public class AccessMethodsForNumber extends JDBCDatabaseMethods implements ValueAccessMethods<Double> {
	
	/**
	 * Construct an access method object.
	 */
	public AccessMethodsForNumber() {
	}

	private PreparedStatement select_range;
	private static final String SELECT_RANGE = 
		"select min(date), max(date) from " + DB.VALUE_DOUBLE + " where series = ?";
	@Override
	public Range getRange(Series<Double> series) throws T2DBException {
		Range range = null;
		TimeDomain timeDomain = series.getTimeDomain();
		try {
			select_range = open(SELECT_RANGE, series, select_range);
			select_range.setInt(1, getId(series));
			ResultSet rs = select_range.executeQuery();
			if (rs.next()) {
				range = new Range(timeDomain.timeFromOffset(rs.getInt(1)), timeDomain.timeFromOffset(rs.getInt(2)));
				if (rs.wasNull())
					range = null;
			}
		} catch (Exception e) {
			throw T2DBJMsg.exception(e, D.D50122, series.getName(true));
		} finally {
			select_range = close(select_range);
		}
		if (range == null)
			range = new Range(series.getTimeDomain());
		return range;
	}
	
	private PreparedStatement select_double_by_range;
	private static final String SELECT_DOUBLE_BY_RANGE = 
		"select date, element from " + DB.VALUE_DOUBLE + 
		" where series = ? and date between ? and ? order by date";
	private PreparedStatement select_double;
	private static final String SELECT_DOUBLE = 
		"select date, element from " + DB.VALUE_DOUBLE + " where series = ? order by date";
	@Override
	public long getValues(Series<Double> series, Range range, TimeAddressable<Double> ts) throws T2DBException {
		if (range != null && range.isEmpty())
			return 0;
		long count = 0;
		try {
			check(Permission.READ, series);
			ResultSet rs;
			if (range == null) {
				select_double = open(SELECT_DOUBLE, series, select_double);
				select_double.setInt(1, getId(series));
				rs = select_double.executeQuery();
			} else {
				select_double_by_range = open(SELECT_DOUBLE_BY_RANGE, series, select_double_by_range);
				select_double_by_range.setInt(1, getId(series));
				select_double_by_range.setInt(2, range.getFirst().asOffset());
				select_double_by_range.setInt(3, range.getLast().asOffset());
				rs = select_double_by_range.executeQuery();
			}
			while (rs.next()) {
				ts.put(ts.getTimeDomain().timeFromOffset(rs.getInt(1)), rs.getDouble(2));
				count++;
			}
		} catch (Exception e) {
			if (range == null)
				throw T2DBJMsg.exception(e, D.D50121, series.getName(true));
			else
				throw T2DBJMsg.exception(e, D.D50120, series.getName(true), range.toString());
		} finally {
			select_double = close(select_double);
			select_double_by_range = close(select_double_by_range);
		}
		return count;
	}
	
	private PreparedStatement select_first_double1;
	private static final String SELECT_FIRST_DOUBLE_1 = 
		"select date, element from " +  DB.VALUE_DOUBLE + " where series = ? and date = " + 
		"(select min(date) from " + DB.VALUE_DOUBLE + " where series = ? and date >= ?)" ;
	private PreparedStatement select_first_double2;
	private static final String SELECT_FIRST_DOUBLE_2 = 
		"select date, element from " +  DB.VALUE_DOUBLE + " where series = ? and date = " + 
		"(select min(date) from " + DB.VALUE_DOUBLE + " where series = ?)" ;
	@Override
	public Observation<Double> getFirst(Series<Double> series, TimeIndex time) throws T2DBException {
		Observation<Double> obs = null;
		try {
			check(Permission.READ, series);
			ResultSet rs;
			int sid = getId(series);
			if (time != null) {
				select_first_double1 = open(SELECT_FIRST_DOUBLE_1, series, select_first_double1);
				select_first_double1.setInt(1, sid);
				select_first_double1.setInt(2, sid);
				select_first_double1.setInt(3, time.asOffset());
				rs = select_first_double1.executeQuery();
			} else {
				select_first_double2 = open(SELECT_FIRST_DOUBLE_2, series, select_first_double2);
				select_first_double2.setInt(1, sid);
				select_first_double2.setInt(2, sid);
				rs = select_first_double2.executeQuery();
			}
			if (rs.next()) {
				TimeDomain dom = time == null ? series.getTimeDomain() : time.getTimeDomain();
				TimeIndex t = dom.timeFromOffset(rs.getInt(1));
				obs = new Observation<Double>(t, rs.getDouble(2));
			}
		} catch (Exception e) {
				throw T2DBJMsg.exception(e, D.D50123, series.getName(true), time.toString());
		} finally {
			select_first_double1 = close(select_first_double1);
			select_first_double2 = close(select_first_double2);
		}
		return obs;
	}
	
	private PreparedStatement select_last_double1;
	private static final String SELECT_LAST_DOUBLE_1 = 
		"select date, element from " +  DB.VALUE_DOUBLE + " where series = ? and date = " + 
		"(select max(date) from " + DB.VALUE_DOUBLE + " where series = ? and date <= ?)" ;
	private PreparedStatement select_last_double2;
	private static final String SELECT_LAST_DOUBLE_2 = 
		"select date, element from " +  DB.VALUE_DOUBLE + " where series = ? and date = " + 
		"(select max(date) from " + DB.VALUE_DOUBLE + " where series = ?)" ;
	@Override
	public Observation<Double> getLast(Series<Double> series, TimeIndex time) throws T2DBException {
		Observation<Double> obs = null;
		try {
			check(Permission.READ, series);
			ResultSet rs;
			int sid = getId(series);
			if (time != null) {
				select_last_double1 = open(SELECT_LAST_DOUBLE_1, series, select_last_double1);
				select_last_double1.setInt(1, sid);
				select_last_double1.setInt(2, sid);
				select_last_double1.setInt(3, time.asOffset());
				rs = select_last_double1.executeQuery();
			} else {
				select_last_double2 = open(SELECT_LAST_DOUBLE_2, series, select_last_double2);
				select_last_double2.setInt(1, sid);
				select_last_double2.setInt(2, sid);
				rs = select_last_double2.executeQuery();
			}
			if (rs.next()) {
				TimeDomain dom = time == null ? series.getTimeDomain() : time.getTimeDomain();
				TimeIndex t = dom.timeFromOffset(rs.getInt(1));
				obs = new Observation<Double>(t, rs.getDouble(2));
			}
		} catch (Exception e) {
				throw T2DBJMsg.exception(e, D.D50124, series.getName(true), time.toString());
		} finally {
			select_last_double1 = close(select_last_double1);
			select_last_double2 = close(select_last_double2);
		}
		return obs;
	}

	private PreparedStatement delete_values_by_t; 
	private static final String DELETE_VALUES_BY_T = 
		"delete from " + DB.VALUE_DOUBLE + " where series = ? and date = ?";
	@Override
	public boolean deleteValue(UpdatableSeries<Double> series, TimeIndex t, ChronicleUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		try {
			check(Permission.MODIFY, series);
			policy.willDelete(series, t);
			policy.deleteValue(series, t);
			delete_values_by_t = open(DELETE_VALUES_BY_T, series, delete_values_by_t);
			delete_values_by_t.setInt(1, getId(series));
			delete_values_by_t.setInt(2, t.asOffset());
			delete_values_by_t.execute();
			done = delete_values_by_t.getUpdateCount() > 0;
		} catch (Exception e) {
			throw T2DBJMsg.exception(e, J.J50113, series.getName(true), t.toString());
		} finally {
			delete_values_by_t = close(delete_values_by_t);
		}
		return done;
	}
	
	private PreparedStatement update_series_range;
	private static final String UPDATE_SERIES_RANGE = 
		"delete from " + DB.VALUE_DOUBLE + " where series = ? and (date < ? or date > ?)";
	@Override
	public boolean updateSeries(UpdatableSeries<Double> series, Range range, ChronicleUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		try {
			int id = getId(series);
			int first = 1;
			int last = 0;
			if (!range.isEmpty()) {
				first = range.getFirst().asOffset();
				last = range.getLast().asOffset();
			}
			check(Permission.MODIFY, series);
			policy.willUpdate(series, range);
			done = policy.update(series, range);
			update_series_range = open(UPDATE_SERIES_RANGE, series, update_series_range);
			update_series_range.setInt(1, id);
			update_series_range.setInt(2, first);
			update_series_range.setInt(3, last);
			update_series_range.execute();
			if (update_series_range.getUpdateCount() > 0)
				done = true;
		} catch (Exception e) {
			throw T2DBJMsg.exception(e, J.J50109, series.getName(true));
		} finally {
			update_series_range = close(update_series_range);
		}
		return done;
	}
	
	private PreparedStatement insert_value;
	private static final String INSERT_VALUE = 
		"insert into " + DB.VALUE_DOUBLE + " (series, date, element) values(?, ?, ?)";
	/**
	 * An IllegalArgumentException is thrown if the observation's value is null or a NaN.
	 * @param series a series 
	 * @param obs an observation
	 * @param policy a policy
	 * @return true if something done
	 * @throws T2DBException
	 */
	protected boolean insertOrUpdateValue(UpdatableSeries<Double> series, Observation<Double> obs, ChronicleUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		Double v = obs.getValue();
		if (v == null || Double.isNaN(v))
			throw new IllegalArgumentException("value null or NaN");
		try {
			check(Permission.MODIFY, series);
			insert_value = open(INSERT_VALUE, series, insert_value);
			insert_value.setInt(1, getId(series));
			insert_value.setInt(2, obs.getTime().asOffset());
			insert_value.setDouble(3, v);
			insert_value.execute();
			done = insert_value.getUpdateCount() > 0;
		} catch (SQLException e) {
			done = updateValue(e, series, obs, policy);
		} catch (KeyedException e) {
			throw T2DBJMsg.exception(e, J.J50110, series.getName(true), obs.getTime().toString());
		} finally {
			insert_value = close(insert_value);
		}
		return done;
	}
	
	private PreparedStatement update_value;
	private static final String UPDATE_VALUE = 
		"update " + DB.VALUE_DOUBLE + " set element = ? where series = ? and date = ? and element != ?";
	private boolean updateValue(SQLException originalException, UpdatableSeries<Double> series, Observation<Double> obs, ChronicleUpdatePolicy policy) throws T2DBException {
		boolean done = false;
		try {
			check(Permission.MODIFY, series);
			update_value = open(UPDATE_VALUE, series, update_value);
			update_value.setDouble(1, obs.getValue());
			update_value.setInt(2, getId(series));
			update_value.setInt(3, obs.getTime().asOffset());
			update_value.setDouble(4, obs.getValue());
			update_value.execute();
			done = update_value.getUpdateCount() > 0;
		} catch (Exception e) {
			throw T2DBJMsg.exception(originalException, J.J50110, series.getName(true), obs.getTime().toString());
		} finally {
			update_value = close(update_value);
		}
		return done;
	}
	
//	private PreparedStatement insertOrUpdate;
//	private static final String INSERT_OR_UPDATE = 
//		"insert into " + DB.VALUE + " (series, date, element) values(?, ?, ?) " +
//		"on duplicate key update element = ?";
//	@Override
//	public void updateValue(UpdatableSeries<Double> series, Observation<Double> obs, ChronicleUpdatePolicy policy) throws KeyedException {
//		boolean done = false;
//		if (Double.isNaN(obs.getValue())) {
//			deleteValue(series, obs.getTime(), policy);
//			done = true;
//		} else {
//			try {
//				check(Permission.MODIFY, series);
//				insertOrUpdate = open(INSERT_OR_UPDATE, series, insertOrUpdate);
//				insertOrUpdate.setInt(1, getId(series));
//				insertOrUpdate.setInt(2, obs.getTime().asOffset());
//				insertOrUpdate.setDouble(3, obs.getValue());
//				insertOrUpdate.setDouble(4, obs.getValue());
//				insertOrUpdate.execute();
//				done = insertOrUpdate.getUpdateCount() > 0;
//			} catch (SQLException e) {
//				throw new LegacyException(e, 10030);
//			} finally {
//				insertOrUpdate = close(insertOrUpdate);
//			}
//		}
//		if (!done)
//			throw new LegacyException(409874, series.getName(true), obs.getTime().toString());
//	}

	@Override
	public long updateValues(UpdatableSeries<Double> series, TimeAddressable<Double> values, ChronicleUpdatePolicy policy) throws T2DBException {
		long count = 0;
		for(Observation<Double> obs : values) {
			boolean done = false;
			if (values.isMissing(obs.getValue()))
				done = deleteValue(series, obs.getTime(), policy);
			else
				done = insertOrUpdateValue(series, obs, policy);
			if (done)
				count++;
		}
		return count;
	}
	
}
