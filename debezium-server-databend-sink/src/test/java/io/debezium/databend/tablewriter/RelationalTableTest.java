/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.databend.tablewriter;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.debezium.DebeziumException;
import io.debezium.databend.testresources.TargetPostgresqlDB;

import java.sql.*;
import java.util.LinkedHashMap;

import com.mongodb.assertions.Assertions;
import io.debezium.server.databend.tablewriter.RelationalTable;

import static io.debezium.server.databend.DatabendChangeConsumer.mapper;

import org.jooq.CreateTableColumnStep;
import org.jooq.CreateTableConstraintStep;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.jooq.impl.SQLDataType.*;

class RelationalTableTest {
    public static TargetPostgresqlDB db = new TargetPostgresqlDB();
    public static Connection connection;

    // need use databend to test this
    public static Connection CreateConnection()
            throws SQLException {
        String url = db.container.getJdbcUrl();
        return DriverManager.getConnection(url, db.container.getUsername(), db.container.getPassword());
    }

    @BeforeAll
    static void beforeAll() throws Exception {
        db.start();
//        BasicDataSource dataSource = BasicDataSourceFactory.createDataSource(new Properties());
//        dataSource.setUrl(db.container.getJdbcUrl());
//        dataSource.setUsername(db.container.getUsername());
//        dataSource.setPassword(db.container.getPassword());
//        connection = CreateConnection();

        // CREATE TES TABLE USING JOOQ
        try (Connection conn = DriverManager.getConnection(
                db.container.getJdbcUrl(),
                db.container.getUsername(),
                db.container.getPassword())) {
            DSLContext create = DSL.using(conn);

            try (CreateTableConstraintStep sql = create.createTable("tbl_with_pk")
                    .column("id", BIGINT)
                    .column("coll1", LONGNVARCHAR)
                    .column("coll2", NUMERIC)
                    .column("coll3", FLOAT)
                    .column("coll4", DECIMAL)
                    .primaryKey("id")) {
                sql.execute();
            }

            try (CreateTableColumnStep sql = create.createTable("tbl_without_pk")
                    .column("id", BIGINT)
                    .column("coll1", LONGNVARCHAR)
                    .column("coll2", NUMERIC)
                    .column("coll3", DATE)
                    .column("coll4", DECIMAL)) {
                sql.execute();
            }
        }
    }

    @Test
    void complexTypeBinding() {
        String withPK = "INSERT INTO \"public\".\"tbl_with_pk\" (\"id\", \"coll1\") values (?, ?)";
        try (Connection connection = CreateConnection()) {
            LinkedHashMap<Integer, String> testhashmap = new LinkedHashMap<>();
            testhashmap.put(100, "Amit");
            System.out.println("DATA=" + mapper.writeValueAsString(testhashmap));

            try (PreparedStatement statement = connection.prepareStatement(withPK)) {
                ((PreparedStatement) statement).setInt(1, 1);
                ((PreparedStatement) statement).setString(2, mapper.writeValueAsString(testhashmap));

                int rows = statement.executeUpdate();
                System.out.println("Rows inserted=" + rows);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        } catch (SQLException | JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    void experiment() {
        try (Connection connection = CreateConnection()) {
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("SELECT * FROM tbl_with_pk");

                // 处理结果集
                while (resultSet.next()) {
                    // 读取每行数据的具体字段值
                    int id = resultSet.getInt("id");
                    String coll1 = resultSet.getString("coll1");

                    System.out.println("ID: " + id + ", coll1: " + coll1);
                }

                resultSet.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    void hasPK() {
        RelationalTable tbl_without_pk = new RelationalTable("", "public", "tbl_without_pk", connection);
        RelationalTable tbl_with_pk = new RelationalTable("id", "public", "tbl_with_pk", connection);
        Assertions.assertTrue(tbl_with_pk.hasPK());
        Assertions.assertFalse(tbl_without_pk.hasPK());
    }

    @Test
    void preparedInsertStatement() {
        String withPK = "REPLACE INTO public.tbl_with_pk \n" +
                "(coll3 , coll2 , coll1 , id , coll4 ) \n" +
                "VALUES (:coll3, :coll2, :coll1, :id, :coll4)";
        String withoutPK = "REPLACE INTO \"public\".\"tbl_without_pk\" \n" +
                "(\"coll3\" , \"coll2\" , \"coll1\" , \"id\" , \"coll4\" ) \n" +
                "VALUES (:coll3, :coll2, :coll1, :id, :coll4)";
        RelationalTable tbl_without_pk = new RelationalTable("", "public", "tbl_without_pk", connection);
        RelationalTable tbl_with_pk = new RelationalTable("id", "public", "tbl_with_pk", connection);
        Assert.assertEquals(withPK, tbl_with_pk.preparedInsertStatement(""));
        Assert.assertEquals(withoutPK, tbl_without_pk.preparedInsertStatement("\""));
    }

    @Test
    void preparedDeleteStatement() {
        String withPK = "DELETE FROM public.tbl_with_pk \n" +
                "WHERE id = :id";
        RelationalTable tbl_without_pk = new RelationalTable("", "public", "tbl_without_pk", connection);
        RelationalTable tbl_with_pk = new RelationalTable("id", "public", "tbl_with_pk", connection);
        System.out.println("sjh");
        System.out.println(tbl_with_pk.preparedDeleteStatement(""));
        Assert.assertEquals(withPK, tbl_with_pk.preparedDeleteStatement(""));
        Assert.assertThrows(DebeziumException.class, () -> tbl_without_pk.preparedDeleteStatement(""));
    }
}