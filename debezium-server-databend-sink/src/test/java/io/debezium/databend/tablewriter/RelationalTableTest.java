/*
 *
 *  * Copyright Databend Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.databend.tablewriter;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.debezium.DebeziumException;

import java.sql.*;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Optional;

import io.debezium.databend.testresources.TargetDatabendDB;
import com.mongodb.assertions.Assertions;
import io.debezium.server.databend.tablewriter.RelationalTable;

import static io.debezium.server.databend.DatabendChangeConsumer.mapper;

import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class RelationalTableTest {
    public static Connection connection;

    @BeforeAll
    static void beforeAll() throws Exception {
        // CREATE TES TABLE USING JOOQ
        TargetDatabendDB targetDatabendDB = new TargetDatabendDB();
        targetDatabendDB.start();

        connection = targetDatabendDB.createConnection();
        connection.createStatement().execute("CREATE DATABASE if not exists " + targetDatabendDB.DB_DATABASE);

        String createTableWithPkSql = "CREATE TABLE IF NOT EXISTS " + targetDatabendDB.DB_DATABASE + ".tbl_with_pk (id BIGINT,coll1 VARCHAR, coll2 INT, coll3 FLOAT, coll4 INT);";
        connection.createStatement().execute(createTableWithPkSql);

        String createTableWithoutPkSql = "CREATE TABLE IF NOT EXISTS " + targetDatabendDB.DB_DATABASE + ".tbl_without_pk (id BIGINT,coll1 VARCHAR, coll2 INT, coll3 FLOAT, coll4 INT);";
        connection.createStatement().execute(createTableWithoutPkSql);
    }

    @AfterAll
    static void tearDown() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute("DROP DATABASE public");
    }

    @Test
    void complexTypeBinding() throws SQLException {
        String withPK = "INSERT INTO \"public\".\"tbl_with_pk\" (\"id\", \"coll1\",\"coll2\",\"coll3\",\"coll4\") values (?, ?, ?, ?, ?)";
        LinkedHashMap<Integer, String> testhashmap = new LinkedHashMap<>();
        testhashmap.put(100, "Amit");

        try (PreparedStatement statement = connection.prepareStatement(withPK)) {
            (statement).setInt(1, 1);
            (statement).setString(2, mapper.writeValueAsString(testhashmap));
            statement.setInt(3, 1);
            statement.setFloat(4, 1);
            statement.setInt(5, 1);
            statement.addBatch();
            int[] ans = statement.executeBatch();

            System.out.println("Rows inserted=" + Arrays.stream(ans).sum());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    void experiment() throws SQLException {
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
        String withPK = "REPLACE INTO public.tbl_with_pk on(id)\n" +
                "VALUES (?, ?, ?, ?, ?)";
        String withoutPK = "INSERT INTO \"public\".\"tbl_without_pk\" \n" +
                "VALUES (?, ?, ?, ?, ?)";
        RelationalTable tbl_without_pk = new RelationalTable("", "public", "tbl_without_pk", connection);
        RelationalTable tbl_with_pk = new RelationalTable("id", "public", "tbl_with_pk", connection);
        System.out.println(tbl_with_pk.preparedUpsertStatement(""));
        Assert.assertEquals(withPK, tbl_with_pk.preparedUpsertStatement(""));
        Assert.assertEquals(withoutPK, tbl_without_pk.prepareInsertStatement("\""));
    }

    @Test
    void preparedDeleteStatement() {
        String withPK = "DELETE FROM public.tbl_with_pk \n" +
                "WHERE id = :id";
        RelationalTable tbl_without_pk = new RelationalTable("", "public", "tbl_without_pk", connection);
        RelationalTable tbl_with_pk = new RelationalTable("id", "public", "tbl_with_pk", connection);
        System.out.println(tbl_with_pk.preparedDeleteStatement("", ":id"));
        Assert.assertEquals(withPK, tbl_with_pk.preparedDeleteStatement("", ":id"));
        Assert.assertThrows(DebeziumException.class, () -> tbl_without_pk.preparedDeleteStatement("", ":id"));
    }
}