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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.janino.JaninoHandlerCompilerAndCacheUtil;

import java.lang.reflect.Proxy;

import static java.util.Objects.requireNonNull;

/**
 * Provides metadata handlers generated via Janino.
 */
public class JaninoMetadataHandlerProvider implements MetadataHandlerProvider {
  private final RelMetadataProvider relMetadataProvider;

  public JaninoMetadataHandlerProvider(RelMetadataProvider relMetadataProvider) {
    this.relMetadataProvider = relMetadataProvider;
  }

  @Override public <MH extends MetadataHandler<?>> MH initialHandler(Class<MH> handlerClass) {
    return handlerClass.cast(
        Proxy.newProxyInstance(RelMetadataQuery.class.getClassLoader(),
            new Class[] {handlerClass}, (proxy, method, args) -> {
              final RelNode r = requireNonNull((RelNode) args[0], "(RelNode) args[0]");
              throw new NoHandler(r.getClass());
            }));
  }

  @Override public <MH extends MetadataHandler<?>> MH revise(Class<MH> handlerClass) {
    return JaninoHandlerCompilerAndCacheUtil.getHandler(relMetadataProvider, handlerClass);
  }

  @Override public MetadataCache buildCache() {
    return new TableMetadataCache();
  }
}