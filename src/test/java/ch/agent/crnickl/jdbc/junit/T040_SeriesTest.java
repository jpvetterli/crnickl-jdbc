package ch.agent.crnickl.jdbc.junit;

import ch.agent.crnickl.junit.Context;

public class T040_SeriesTest extends ch.agent.crnickl.junit.T040_SeriesTest {
	@Override
	protected Context getContext() {
		return JDBCContext.getInstance();
	}
}