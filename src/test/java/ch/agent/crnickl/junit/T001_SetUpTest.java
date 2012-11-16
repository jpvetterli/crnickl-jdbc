package ch.agent.crnickl.junit;

import junit.framework.TestCase;

public class T001_SetUpTest extends TestCase {

	@Override
	protected void setUp() throws Exception {
		DBSetUp.getDatabase();
	}

	public void test_setUp() {
		assertEquals("setup okay", "setup okay");
	}

	
}
