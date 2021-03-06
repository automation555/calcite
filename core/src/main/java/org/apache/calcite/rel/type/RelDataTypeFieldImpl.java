/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.type;

import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.Serializable;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Default implementation of {@link RelDataTypeField}.
 */
public class RelDataTypeFieldImpl implements RelDataTypeField, Serializable {
  //~ Instance fields --------------------------------------------------------

  private final RelDataType type;
  private final String name;
  private final int index;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a RelDataTypeFieldImpl.
   */
  public RelDataTypeFieldImpl(
      String name,
      int index,
      RelDataType type) {
    this.name = requireNonNull(name, "name");
    this.index = index;
    this.type = requireNonNull(type, "type");
  }

  //~ Methods ----------------------------------------------------------------

  @Override public int hashCode() {
    return Objects.hash(index, name, type);
  }

  @Override public boolean equals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof RelDataTypeFieldImpl)) {
      return false;
    }
    RelDataTypeFieldImpl that = (RelDataTypeFieldImpl) obj;
    return this.index == that.index
        && this.name.equals(that.name)
        && this.type.equals(that.type);
  }

  // implement RelDataTypeField
  @Override public String getName() {
    return name;
  }

  // implement RelDataTypeField
  @Override public int getIndex() {
    return index;
  }

  // implement RelDataTypeField
  @Override public RelDataType getType() {
    return type;
  }

  // implement Map.Entry
  @Override public final String getKey() {
    return getName();
  }

  // implement Map.Entry
  @Override public final RelDataType getValue() {
    return getType();
  }

  // implement Map.Entry
  @Override public RelDataType setValue(RelDataType value) {
    throw new UnsupportedOperationException();
  }

  // for debugging
  @Override public String toString() {
    return "#" + index + ": " + name + " " + type;
  }

  @Override public boolean isDynamicStar() {
    return type.getSqlTypeName() == SqlTypeName.DYNAMIC_STAR;
  }

}
