package ch.agent.crnickl.jdbc.junit;

import ch.agent.crnickl.api.DBObjectId;
import ch.agent.crnickl.jdbc.JDBCObjectId;
import ch.agent.crnickl.junit.Context;

public class T005_BasicTest extends ch.agent.crnickl.junit.T005_BasicTest {
	
	protected DBObjectId id(int id) {
		return new JDBCObjectId(id);
	}

	@Override
	protected Context getContext() {
		return JDBCContext.getInstance();
	}

}
