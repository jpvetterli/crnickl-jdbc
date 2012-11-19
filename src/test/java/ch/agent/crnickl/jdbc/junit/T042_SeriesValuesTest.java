package ch.agent.crnickl.jdbc.junit;

import ch.agent.crnickl.junit.Context;

public class T042_SeriesValuesTest extends ch.agent.crnickl.junit.T042_SeriesValuesTest {
	@Override
	protected Context getContext() {
		return JDBCContext.getInstance();
	}
}