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
		public static final String J00110 = "J00110"; 
		public static final String J00111 = "J00111"; 
		
		public static final String J01101 = "J01101";
		
		public static final String J10104 = "J10104";
		public static final String J10105 = "J10105";
		public static final String J10106 = "J10106";
		public static final String J10107 = "J10107";
		public static final String J10110 = "J10110"; 
		public static final String J10114 = "J10114"; 
		public static final String J10121 = "J10121"; 
		public static final String J10122 = "J10122"; 
		public static final String J10126 = "J10126"; 

		public static final String J20104 = "J20104"; 
		public static final String J20105 = "J20105"; 
		public static final String J20106 = "J20106"; 
		public static final String J20107 = "J20107"; 
		public static final String J20109 = "J20109"; 
		public static final String J20116 = "J20116";
		public static final String J20120 = "J20120"; 
		
		public static final String J30104 = "J30104"; 
		public static final String J30105 = "J30105"; 
		public static final String J30109 = "J30109"; 
		public static final String J30114 = "J30114"; 
		public static final String J30116 = "J30116"; 
		public static final String J30117 = "J30117";
		public static final String J30122 = "J30122";
		public static final String J30123 = "J30123";
		public static final String J30124 = "J30124";
		public static final String J30125 = "J30125";
		public static final String J30126 = "J30126";
		public static final String J30127 = "J30127";
		public static final String J30128 = "J30128";
		public static final String J30129 = "J30129";
		public static final String J30130 = "J30130";

		public static final String J40104 = "J40104"; 
		public static final String J40105 = "J40105"; 
		public static final String J40106 = "J40106"; 
		public static final String J40109 = "J40109"; 
		public static final String J40110 = "J40110"; 
		public static final String J40111 = "J40111"; 
		public static final String J40112 = "J40112"; 
		public static final String J40113 = "J40113"; 
		public static final String J40114 = "J40114"; 
		public static final String J40119 = "J40119"; 
		public static final String J40120 = "J40120"; 
		public static final String J40121 = "J40121"; 
		public static final String J40122 = "J40122"; 
		public static final String J40123 = "J40123"; 
		
		public static final String J50104 = "J50104"; 
		public static final String J50109 = "J50109"; 
		public static final String J50110 = "J50110"; 
		public static final String J50111 = "J50111"; 
		public static final String J50112 = "J50112"; 
		public static final String J50113 = "J50113"; 
		public static final String J50119 = "J50119"; 
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
