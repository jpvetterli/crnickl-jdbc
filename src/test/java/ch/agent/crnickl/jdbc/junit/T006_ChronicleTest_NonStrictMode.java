package ch.agent.crnickl.jdbc.junit;

import ch.agent.crnickl.junit.Context;

public class T006_ChronicleTest_NonStrictMode extends ch.agent.crnickl.junit.T006_ChronicleTest_NonStrictMode {
	@Override
	protected Context getContext() {
		return JDBCContext.getInstance();
	}
}