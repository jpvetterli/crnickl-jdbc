package ch.agent.crnickl.jdbc.junit;

import ch.agent.crnickl.junit.Context;

public class T012_ValueTypeTest extends ch.agent.crnickl.junit.T012_ValueTypeTest {
	@Override
	protected Context getContext() {
		return JDBCContext.getInstance();
	}
}
