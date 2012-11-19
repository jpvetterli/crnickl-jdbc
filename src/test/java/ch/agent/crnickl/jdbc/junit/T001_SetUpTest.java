package ch.agent.crnickl.jdbc.junit;

import ch.agent.crnickl.junit.Context;

public class T001_SetUpTest extends ch.agent.crnickl.junit.T001_SetUpTest {
	@Override
	protected Context getContext() {
		return JDBCContext.getInstance();
	}
}
