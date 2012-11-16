package ch.agent.crnickl.junit;

import java.lang.reflect.Method;

import junit.framework.TestCase;
import ch.agent.core.KeyedException;

public class AbstractTest extends TestCase {
	
	private static final String EXCEPTION_EXPECTED = "Exception expected";
	private static final String NOT_KEYED = "Not keyed (%s): %s";
	
	private static int testCount= -1;
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		if (testCount < 0) {
			testCount = 0;
			for (Method m : this.getClass().getMethods()) {
				if (m.getName().startsWith("test"))
					testCount++;
			}
			firstSetUp();
		}
	}
	
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		if (testCount > 0) {
			testCount--;
			if (testCount == 0) {
				lastTearDown();
				testCount = -1;
			}
		}
	}

	/**
	 * This method is called by {@link setUp} before the first test. 
	 * So if you override <code>setUp</code>, you must call super.
	 * By default the method does nothing.
	 * 
	 * @throws Exception
	 */
	protected void firstSetUp()  throws Exception {
	}
	
	/**
	 * This method is called by {@link tearDown} after the last test. 
	 * So if you override <code>tearDown</code>, you must call super.
	 * By default the method does nothing.
	 * 
	 * @throws Exception
	 */
	protected void lastTearDown()  throws Exception {
	}

	/**
	 * Fail with a message stating that an exception should have been thrown.
	 */
	protected static void expectException() {
		fail(EXCEPTION_EXPECTED);
	}
	
	/**
	 * Verify an exception and its cause chain against a series of keys. 
	 * Verification fails unless the exception is an instance of
	 * {@link KeyedException} and its key matches.
	 * A null key in the argument list skips a level in the cause chain.
	 * For example the following line asserts that the cause
	 * of exception e has a key equal to "foo":
	 * <blockquote>
	 * <pre><code>
	 * assertException(e, null, "foo");
	 * </code></pre>
	 * </blockquote>
	 * Skipping all levels is equivalent to failing on the 
	 * original exception. For example the two following lines
	 * have the same effect:
	 * <blockquote>
	 * <pre><code>
	 * assertException(new RuntimeException("foo"));
	 * fail(new RuntimeException("foo").toString());
	 * </code></pre>
	 * </blockquote>
	 * 
	 * @param e an exception
	 * @param keys a list of strings
	 */
	protected static void assertException(Exception e, String... keys) {
		Throwable t = e;
		boolean allNull = true;
		for (String key : keys) {
			if (key != null) {
				allNull = false;
				if (t instanceof KeyedException) {
					assertEquals(key, ((KeyedException) t).getMsg().getKey());
				} else {
					fail(String.format(NOT_KEYED, key, t == null ? "null" : t.toString()));
				}
			}
			t = t == null ? null : t.getCause();
		}
		if (allNull)
			fail(e.toString());
	}

}
