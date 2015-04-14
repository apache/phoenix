package org.apache.phoenix.schema.json;

public class PhoenixJsonException extends Exception {

	private static final long serialVersionUID = 1L;

	public PhoenixJsonException(String message, Throwable cause) {
		super(message, cause);
	}

	public PhoenixJsonException(String message) {
		super(message);
	}

}
