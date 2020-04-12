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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveCatalog extends BaseMetastoreCatalog implements Closeable, SupportsNamespaces {
  private static final Logger LOG = LoggerFactory.getLogger(HiveCatalog.class);

  private final HiveClientPool clients;
  private final Configuration conf;
  private static final Joiner SLASH = Joiner.on("/");
  private final StackTraceElement[] createStack;
  private boolean closed;

  public HiveCatalog(Configuration conf) {
    this.clients = new HiveClientPool(conf);
    this.conf = conf;
    this.createStack = Thread.currentThread().getStackTrace();
    this.closed = false;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    Preconditions.checkArgument(namespace.levels().length == 1,
        "Missing database in namespace: %s", namespace);
    String database = namespace.level(0);

    try {
      List<String> tables = clients.run(client -> client.getAllTables(database));
      return tables.stream()
          .map(t -> TableIdentifier.of(namespace, t))
          .collect(Collectors.toList());

    } catch (UnknownDBException e) {
      throw new NotFoundException(e, "Unknown namespace " + namespace.toString());

    } catch (TException e) {
      throw new RuntimeException("Failed to list all tables under namespace " + namespace.toString(), e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to listTables", e);
    }
  }

  @Override
  protected String name() {
    return "hive";
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    if (!isValidIdentifier(identifier)) {
      throw new NoSuchTableException("Invalid identifier: %s", identifier);
    }

    String database = identifier.namespace().level(0);

    TableOperations ops = newTableOps(identifier);
    TableMetadata lastMetadata;
    if (purge && ops.current() != null) {
      lastMetadata = ops.current();
    } else {
      lastMetadata = null;
    }

    try {
      clients.run(client -> {
        client.dropTable(database, identifier.name(),
            false /* do not delete data */,
            false /* throw NoSuchObjectException if the table doesn't exist */);
        return null;
      });

      if (purge && lastMetadata != null) {
        dropTableData(ops.io(), lastMetadata);
      }

      return true;

    } catch (NoSuchObjectException e) {
      return false;

    } catch (TException e) {
      throw new RuntimeException("Failed to drop " + identifier.toString(), e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to dropTable", e);
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    if (!isValidIdentifier(from)) {
      throw new NoSuchTableException("Invalid identifier: %s", from);
    }
    Preconditions.checkArgument(isValidIdentifier(to), "Invalid identifier: %s", to);

    String toDatabase = to.namespace().level(0);
    String fromDatabase = from.namespace().level(0);
    String fromName = from.name();

    try {
      Table table = clients.run(client -> client.getTable(fromDatabase, fromName));
      table.setDbName(toDatabase);
      table.setTableName(to.name());

      clients.run(client -> {
        client.alter_table(fromDatabase, fromName, table);
        return null;
      });

    } catch (NoSuchObjectException e) {
      throw new NoSuchTableException("Table does not exist: %s", from);

    } catch (AlreadyExistsException e) {
      throw new org.apache.iceberg.exceptions.AlreadyExistsException("Table already exists: %s", to);

    } catch (TException e) {
      throw new RuntimeException("Failed to rename " + from.toString() + " to " + to.toString(), e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to rename", e);
    }
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> meta) {
    Preconditions.checkArgument(!namespace.isEmpty(),
        "Cannot create namespace with invalid name: %s", namespace.toString());
    Preconditions.checkArgument(namespace.levels().length == 1,
        "Cannot support multi part namespace in Hive MetaStore: %s", namespace.toString());
    try {
      clients.run(client -> {
        client.createDatabase(convertToDatabase(namespace, meta));
        return null;
      });

    } catch (AlreadyExistsException e) {
      throw new org.apache.iceberg.exceptions.AlreadyExistsException("Namespace '%s' already exists!",
            namespace.toString());

    } catch (TException e) {
      throw new RuntimeException("Failed to create namespace " + namespace.toString() + " in Hive MataStore", e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to createDatabase(name)" + namespace.toString(), e);
    }
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) {
    if (namespace.levels().length > 1) {
      throw new NoSuchNamespaceException("Namespace does not exist: " + namespace.toString());
    }

    try {
      return namespace.isEmpty() ? clients.run(HiveMetaStoreClient::getAllDatabases)
          .stream().map(name -> Namespace.of(name))
          .collect(Collectors.toList()) : clients.run(client -> client.getDatabases(namespace.level(0)))
          .stream().map(name -> Namespace.of(name))
          .collect(Collectors.toList());
    } catch (TException e) {
      throw new RuntimeException("Failed to list all namespace: " + namespace.toString(),  e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to getAllDatabases()" + namespace.toString(), e);
    }
  }

  @Override
  public boolean dropNamespace(Namespace namespace) {
    if (namespace.levels().length != 1) {
      throw new NoSuchNamespaceException("Namespace does not exist: " + namespace.toString());
    }

    try {
      clients.run(client -> {
        client.dropDatabase(namespace.level(0),
            false /* deleteData */,
            false /* ignoreUnknownDb */,
            true /* cascade */);
        return null;
      });

      return true;

    } catch (NoSuchObjectException e) {
      throw new NoSuchNamespaceException("Namespace does not exist: " + namespace.toString(), e.getMessage());

    } catch (TException e) {
      throw new RuntimeException("Failed to drop namespace " + namespace.toString(), e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to drop dropDatabase(name)" + namespace.toString(), e);
    }
  }

  @Override
  public boolean setNamespaceMetadata(Namespace namespace, Map<String, String> meta) {
    Map<String, String> currentMeta = loadNamespaceMetadata(namespace);
    if (meta.containsKey("location")) {
      throw new UnsupportedOperationException(
          "Does not support change 'location' in HiveCatalog: " + namespace.toString());
    }
    if (meta.containsKey("name")) {
      throw new UnsupportedOperationException(
          "Does not support change 'name' in HiveCatalog: " + namespace.toString());
    }

    meta.forEach(currentMeta::put);
    Database currentDb = convertToDatabase(namespace, currentMeta);
    try {
      clients.run(client -> {
        client.alterDatabase(namespace.toString(), currentDb);
        return null;
      });
      return true;
    } catch (NoSuchObjectException e) {
      throw new NoSuchNamespaceException("Namespace does not exist: " + namespace.toString(), e.getMessage());

    } catch (UnknownDBException e) {
      throw new NoSuchNamespaceException("Namespace does not exist: " + namespace.toString(), e.getMessage());

    } catch (TException e) {
      throw new RuntimeException("Failed to list namespace under namespace: %s" + namespace.toString(), e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to getDatabase(name)" + namespace.toString(), e);
    }

  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
    if (namespace.levels().length != 1) {
      throw new NoSuchNamespaceException("Namespace does not exist: " + namespace.toString());
    }

    try {
      Database database = clients.run(client -> client.getDatabase(namespace.toString()));
      return convertToMetadata(database);

    } catch (NoSuchObjectException e) {
      throw new NoSuchNamespaceException("Namespace does not exist: " + namespace.toString(), e.getMessage());

    } catch (UnknownDBException e) {
      throw new NoSuchNamespaceException("Namespace does not exist: " + namespace.toString(), e.getMessage());

    } catch (TException e) {
      throw new RuntimeException("Failed to list namespace under namespace: %s" + namespace.toString(), e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to getDatabase(name)" + namespace.toString(), e);
    }
  }

  @Override
  protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
    return tableIdentifier.namespace().levels().length == 1;
  }

  @Override
  public TableOperations newTableOps(TableIdentifier tableIdentifier) {
    String dbName = tableIdentifier.namespace().level(0);
    String tableName = tableIdentifier.name();
    return new HiveTableOperations(conf, clients, dbName, tableName);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    String warehouseLocation = conf.get("hive.metastore.warehouse.dir");
    Preconditions.checkNotNull(
        warehouseLocation,
        "Warehouse location is not set: hive.metastore.warehouse.dir=null");
    return String.format(
        "%s/%s.db/%s",
        warehouseLocation,
        tableIdentifier.namespace().levels()[0],
        tableIdentifier.name());
  }

  public  Map<String, String> convertToMetadata(Database database) {
    Map<String, String> nameMap = Maps.newHashMap();

    nameMap.put("name", database.getName());
    nameMap.put("location", database.getLocationUri());
    nameMap.put("comment", database.getDescription());
    database.getParameters().forEach(nameMap::put);

    return nameMap;
  }

  public  Database convertToDatabase(Namespace namespace, Map<String, String> meta) {
    String warehouseLocation = conf.get("hive.metastore.warehouse.dir");

    if (namespace.levels().length != 1) {
      throw new NoSuchNamespaceException("Namespace does not exist: " + namespace.toString());
    }
    if (!meta.containsKey("location")) {
      Preconditions.checkNotNull(warehouseLocation,
          "Warehouse location is not set: hive.metastore.warehouse.dir=null");
    }

    Database database  = new Database();
    Map<String, String> parameter = Maps.newHashMap();

    database.setName(namespace.toString());
    database.setLocationUri(SLASH.join(warehouseLocation, SLASH.join(namespace.levels())) + ".db");

    meta.forEach((key, value) -> {
      if (key.equals("comment")) {
        database.setDescription(value);
      } else if (key.equals("location")) {
        database.setLocationUri(value);
      } else {
        parameter.put(key, value);
      }
    });
    database.setParameters(parameter);

    return database;
  }

  @Override
  public void close() {
    if (!closed) {
      clients.close();
      closed = true;
    }
  }

  @SuppressWarnings("checkstyle:NoFinalizer")
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (!closed) {
      close(); // releasing resources is more important than printing the warning
      String trace = Joiner.on("\n\t").join(
          Arrays.copyOfRange(createStack, 1, createStack.length));
      LOG.warn("Unclosed input stream created by:\n\t{}", trace);
    }
  }
}
