package ch.agent.crnickl.jdbc.junit;

import ch.agent.crnickl.junit.Context;

public class T043_SparseSeriesTest extends ch.agent.crnickl.junit.T043_SparseSeriesTest {
	@Override
	protected Context getContext() {
		return JDBCContext.getInstance();
	}
}