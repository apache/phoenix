/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.parse;

import java.util.Objects;

/**
 * POJO carrying the options parsed from an {@code EXPLAIN} statement's option list
 * ({@code EXPLAIN [(<opt> [, <opt>]*)] <stmt>}).
 */
public final class ExplainOptions {

  /** Output format selected by the {@code FORMAT} option. */
  public enum Format {
    TEXT,
    JSON
  }

  public static final ExplainOptions DEFAULT = new ExplainOptions(false, false, Format.TEXT);
  public static final ExplainOptions WITH_REGIONS = new ExplainOptions(true, false, Format.TEXT);
  public static final ExplainOptions VERBOSE = new ExplainOptions(false, true, Format.TEXT);

  private final boolean regions;
  private final boolean verbose;
  private final Format format;

  public ExplainOptions(boolean regions, boolean verbose, Format format) {
    this.regions = regions;
    this.verbose = verbose;
    this.format = format == null ? Format.TEXT : format;
  }

  public boolean isRegions() {
    return regions;
  }

  public boolean isVerbose() {
    return verbose;
  }

  public Format getFormat() {
    return format;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ExplainOptions)) {
      return false;
    }
    ExplainOptions that = (ExplainOptions) o;
    return regions == that.regions && verbose == that.verbose && format == that.format;
  }

  @Override
  public int hashCode() {
    return Objects.hash(regions, verbose, format);
  }

  @Override
  public String toString() {
    return "ExplainOptions{regions=" + regions + ", verbose=" + verbose + ", format=" + format
      + "}";
  }

  /**
   * Mutable builder used by the parser to accumulate options as they are encountered in the option
   * list. Rejects duplicate options.
   */
  public static final class Builder {
    private boolean regions;
    private boolean regionsSet;
    private boolean verbose;
    private boolean verboseSet;
    private Format format;

    public Builder setRegions(boolean regions) {
      if (regionsSet) {
        throw new RuntimeException("Duplicate EXPLAIN option: REGIONS");
      }
      this.regions = regions;
      this.regionsSet = true;
      return this;
    }

    public Builder setVerbose(boolean verbose) {
      if (verboseSet) {
        throw new RuntimeException("Duplicate EXPLAIN option: VERBOSE");
      }
      this.verbose = verbose;
      this.verboseSet = true;
      return this;
    }

    public Builder setFormat(Format format) {
      if (format == null) {
        throw new RuntimeException("EXPLAIN option FORMAT requires a value: TEXT or JSON");
      }
      if (this.format != null) {
        throw new RuntimeException("Duplicate EXPLAIN option: FORMAT");
      }
      this.format = format;
      return this;
    }

    public ExplainOptions build() {
      return new ExplainOptions(regions, verbose, format == null ? Format.TEXT : format);
    }
  }
}
