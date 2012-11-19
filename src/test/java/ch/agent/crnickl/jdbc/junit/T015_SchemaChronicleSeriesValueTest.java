package ch.agent.crnickl.jdbc.junit;

import ch.agent.crnickl.junit.Context;

public class T015_SchemaChronicleSeriesValueTest extends ch.agent.crnickl.junit.T015_SchemaChronicleSeriesValueTest {
	@Override
	protected Context getContext() {
		return JDBCContext.getInstance();
	}
}
