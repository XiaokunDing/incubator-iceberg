/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.catalog;

import java.util.List;
import java.util.Map;

public interface SupportsNamespaces {
  /**
   * Create a namespace in the catalog.
   *
   * @param namespace {@link Namespace}.
   */
  default void createNamespace(Namespace namespace, Map<String, String> meta) {
    throw new UnsupportedOperationException(
        "The catalog" + this.toString()  + "is not Support createNamespace(namespace, meta).");
  }

  /**
   * List top-level namespaces from the catalog.
   * <p>
   * If an object such as a table, view, or function exists, its parent namespaces must also exist
   * and must be returned by this discovery method. For example, if table a.b.t exists, this method
   * must return ["a"] in the result array.
   *
   * @return an List of namespace {@link Namespace} names
   */
  default List<Namespace> listNamespaces() {
    return listNamespaces(Namespace.empty());
  }

  /**
   * List  namespaces from the namespace.
   * <p>
   * For example, if table a.b.t exists, use 'SELECT NAMESPACE IN a' this method
   * must return Namepace.of("b") {@link Namespace}.
   *
   * @return an List of namespace {@link Namespace} names
   */
  default List<Namespace> listNamespaces(Namespace namespace) {
    throw new UnsupportedOperationException(
        "The catalog" + this.toString()  + "is not Support listNamespaces(" + namespace + ").");
  }

  /**
   * Load metadata properties for a namespace.
   *
   * @param namespace a Namespace.of(name) {@link Namespace}
   * @return a string map of properties for the given namespace
   */
  default Map<String, String> loadNamespaceMetadata(Namespace namespace) {
    throw new UnsupportedOperationException(
        "The catalog" + this.toString()  + "is not Support loadNamespaceMetadata(namespace).");
  }

  /**
   * Drop namespace, while the namespace haven't table or sub namespace will return true.
   *
   * @param namespace a Namespace.of(name) {@link Namespace}
   * @return true while drop success.
   */
  default boolean dropNamespace(Namespace namespace, boolean cascade) {
    throw new UnsupportedOperationException(
        "The catalog" + this.toString()  + "is not Support dropNamespace(namespace).");
  }

  default boolean alterNamespace(Namespace namespace) {
    throw new UnsupportedOperationException(
        "The catalog" + this.toString()  + "is not Support alterNamespace(namespace).");
  }
}
