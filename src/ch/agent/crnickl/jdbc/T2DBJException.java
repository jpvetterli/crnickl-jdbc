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
 * Type: T2DBJException
 * Version: 1.0.0
 */
package ch.agent.crnickl.jdbc;

import ch.agent.core.KeyedMessage;
import ch.agent.crnickl.T2DBException;

/**
 * A T2DBJException is thrown by JDBC methods of CrNiCKL when they need to 
 * throw a checked exception.
 * 
 * @author Jean-Paul Vetterli
 * @version 1.0.0
 */
public class T2DBJException extends T2DBException {
	
	private static final long serialVersionUID = 6657865517163634816L;
	
	/**
	 * Construct an exception with a keyed message.
	 * 
	 * @param message a {@link KeyedMessage}
	 */
	public T2DBJException(KeyedMessage message) {
		super(message);
	}
	
	/**
	 * Construct an exception with a keyed message and the causing exception.
	 * 
	 * @param message a {@link KeyedMessage}
	 * @param cause a {@link Throwable}
	 */
	public T2DBJException(KeyedMessage message, Throwable cause) {
		super(message, cause);
	}
}