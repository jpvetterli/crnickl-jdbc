package ch.agent.crnickl.jdbc.junit;

import ch.agent.crnickl.junit.Context;

public class T060_ByAttributeValueTest extends ch.agent.crnickl.junit.T060_ByAttributeValueTest {
	@Override
	protected Context getContext() {
		return JDBCContext.getInstance();
	}
}
