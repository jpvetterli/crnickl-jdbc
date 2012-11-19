package ch.agent.crnickl.jdbc.junit;

import ch.agent.crnickl.junit.Context;

public class T017_SchemaTest extends ch.agent.crnickl.junit.T017_SchemaTest {
	@Override
	protected Context getContext() {
		return JDBCContext.getInstance();
	}
}
