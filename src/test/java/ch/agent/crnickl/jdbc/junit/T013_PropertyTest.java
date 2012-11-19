package ch.agent.crnickl.jdbc.junit;

import ch.agent.crnickl.junit.Context;

public class T013_PropertyTest extends ch.agent.crnickl.junit.T013_PropertyTest {
	@Override
	protected Context getContext() {
		return JDBCContext.getInstance();
	}
}
