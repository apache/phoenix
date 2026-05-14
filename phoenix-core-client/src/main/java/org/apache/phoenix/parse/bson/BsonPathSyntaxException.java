package org.apache.phoenix.parse.bson;

/** Thrown by {@link BsonPathParser} when input does not match the supported JSONPath subset. */
public class BsonPathSyntaxException extends Exception {
  private static final long serialVersionUID = 1L;
  private final int errorOffset;

  public BsonPathSyntaxException(String message, int errorOffset) {
    super(message + " (at offset " + errorOffset + ")");
    this.errorOffset = errorOffset;
  }

  public int getErrorOffset() {
    return errorOffset;
  }
}
