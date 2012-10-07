package ch.agent.crnickl.junit;

import ch.agent.core.KeyedException;
import junit.framework.TestCase;

public class AbstractTest extends TestCase {
	
	private static final String EXCEPTION_EXPECTED = "Exception expected";
	private static final String NOT_KEYED = "Not keyed (%s): %s";
	
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
