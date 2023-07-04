/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.databend.testresources;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;

public class TargetMysqlDB implements QuarkusTestResourceLifecycleManager {

  public static final String DB_USER = "testuser";
  public static final String DB_PASSWORD = "testsecret";
  public static final String DB_DATABASE = "dbdbzdestination";
  private static final Logger LOGGER = LoggerFactory.getLogger(TargetMysqlDB.class);

  static public final MySQLContainer container = new MySQLContainer("mysql")
      .withDatabaseName(DB_DATABASE)
      .withUsername(DB_USER)
      .withPassword(DB_PASSWORD);

  public static void runSQL(String query) throws SQLException, ClassNotFoundException {
    try {
      String url = container.getJdbcUrl();
      Class.forName(container.getDriverClassName());
      Connection con = DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
      Statement st = con.createStatement();
      st.execute(query);
      con.close();
    } catch (Exception e) {
      LOGGER.error(query);
      throw e;
    }
  }

  @Override
  public Map<String, String> start() {
    container.start();

    Map<String, String> config = new ConcurrentHashMap<>();
    config.put("debezium.sink.databend.database.url", container.getJdbcUrl());
    config.put("debezium.sink.databend.database.username", container.getUsername());
    config.put("debezium.sink.databend.database.password", container.getPassword());
    config.put("debezium.sink.databend.database.param.xyz", "val");
    return config;
  }

  @Override
  public void stop() {
    if (container != null) {
      container.stop();
    }
  }

}
