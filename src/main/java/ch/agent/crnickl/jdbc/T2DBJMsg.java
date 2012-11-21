/*
 *   Copyright 2012 Hauser Olsson GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Package: ch.agent.crnickl.jdbc
 * Type: T2DBJMsg
 * Version: 1.0.0
 */
package ch.agent.crnickl.jdbc;

import java.util.ResourceBundle;

import ch.agent.core.KeyedMessage;
import ch.agent.core.MessageBundle;

/**
 * T2DBJMsg provides keyed messages to the package. 
 * 
 * @author Jean-Paul Vetterli
 * @version 1.0.0
 */
public class T2DBJMsg extends KeyedMessage {
	
	/**
	 * Message symbols.
	 */
	public class J {
		public static final String J00101 = "J00101";
		public static final String J00102 = "J00102";
		public static final String J00104 = "J00104";
		public static final String J00105 = "J00105";
		public static final String J00106 = "J00106"; 
		public static final String J00107 = "J00107"; 
		public static final String J00108 = "J00108"; 
		public static final String J00111 = "J00111"; 
		public static final String J01101 = "J01101";
	}
	
	private static final String BUNDLE_NAME = ch.agent.crnickl.jdbc.T2DBJMsg.class.getName();
	
	private static final MessageBundle BUNDLE = new MessageBundle("T2DBJ",
			ResourceBundle.getBundle(BUNDLE_NAME));

	/**
	 * Return a keyed exception.
	 * 
	 * @param key a key
	 * @param arg zero or more arguments
	 * @return a keyed exception
	 */
	public static T2DBJException exception(String key, Object... arg) {
		return new T2DBJException(new T2DBJMsg(key, arg));
	}

	/**
	 * Return a keyed exception.
	 * 
	 * @param cause the exception's cause
	 * @param key a key
	 * @param arg zero or more arguments
	 * @return a keyed exception
	 */
	public static T2DBJException exception(Throwable cause, String key, Object... arg) {
		return new T2DBJException(new T2DBJMsg(key, arg), cause);
	}

	/**
	 * Construct a keyed message.
	 * 
	 * @param key a key
	 * @param args zero or more arguments
	 */
	public T2DBJMsg(String key, Object... args) {
		super(key, BUNDLE, args);
	}

}
