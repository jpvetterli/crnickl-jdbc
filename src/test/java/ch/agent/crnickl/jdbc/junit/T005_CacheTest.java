package ch.agent.crnickl.jdbc.junit;

import ch.agent.crnickl.junit.Context;

public class T005_CacheTest extends ch.agent.crnickl.junit.T005_CacheTest {
	@Override
	protected Context getContext() {
		return JDBCContext.getInstance();
	}
}
