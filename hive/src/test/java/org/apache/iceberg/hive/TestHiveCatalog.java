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

package org.apache.iceberg.hive;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

public class TestHiveCatalog extends HiveMetastoreTest {

  private static Map meta = new HashMap<String, String>() {
    {
      put("owner", "apache");
      put("group", "iceberg");
      put("comment", "iceberg  hiveCatalog test");
    }
  };

  @Test
  public void testCreateNamespace() throws TException {
    Namespace namespace = Namespace.of("dbname");

    catalog.createNamespace(namespace, meta);

    Database database = metastoreClient.getDatabase(namespace.toString());
    Map<String, String> dbMeta = catalog.getMetafrpmhiveDb(database);
    Assert.assertTrue(dbMeta.get("owner").equals("apache"));
    Assert.assertTrue(dbMeta.get("group").equals("iceberg"));
    Assert.assertEquals("there no same location for db and namespace",
        dbMeta.get("location"), catalog.nameSpaceToHiveDb(namespace, meta).getLocationUri());
  }

  @Test
  public void testListNamespace() throws TException {
    Namespace namespace1 = Namespace.of("dbname1");
    catalog.createNamespace(namespace1, meta);

    Namespace namespace2 = Namespace.of("dbname2");
    catalog.createNamespace(namespace2, meta);
    List<Namespace> namespaces = catalog.listNamespaces();
    Assert.assertTrue("hive db not hive the namespace 'dbname1'", namespaces.contains(namespace1));
    Assert.assertTrue("hive db not hive the namespace 'dbname2'", namespaces.contains(namespace2));
  }

  @Test
  public void testLoadNamespaceMeta() throws TException {
    Namespace namespace = Namespace.of("dbname_load");

    catalog.createNamespace(namespace, meta);

    Map<String, String> nameMata = catalog.loadNamespaceMetadata(namespace);
    Assert.assertTrue(nameMata.get("owner").equals("apache"));
    Assert.assertTrue(nameMata.get("group").equals("iceberg"));
    Assert.assertEquals("there no same location for db and namespace",
        nameMata.get("location"), catalog.nameSpaceToHiveDb(namespace, meta).getLocationUri());
  }

  @Test
  public void testDropNamespace() throws TException {

    Namespace namespace = Namespace.of("dbname_drop");
    catalog.createNamespace(namespace, meta);

    Map<String, String> nameMata = catalog.loadNamespaceMetadata(namespace);
    Assert.assertTrue(nameMata.get("owner").equals("apache"));
    Assert.assertTrue(nameMata.get("group").equals("iceberg"));

    Assert.assertTrue("drop namespace " + namespace.toString() + "error", catalog.dropNamespace(namespace, false));
    AssertHelpers.assertThrows("should throw exception", NoSuchNamespaceException.class,
        "namespace does not exist:", () -> {
          catalog.dropNamespace(Namespace.of("db.ns1"), false);
        });
    AssertHelpers.assertThrows("should throw exception" + namespace.toString(),
        org.apache.iceberg.exceptions.NoSuchNamespaceException.class,
        "namespace does not exist: " + namespace.toString(), () -> {
          catalog.loadNamespaceMetadata(namespace);
        });
  }

}
