/*
 *
 *  * Copyright Databend Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.databend.testresources;

import com.databend.jdbc.DatabendResultSet;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author hantmac
 */
public class BaseDbTest {
    @ConfigProperty(name = "debezium.sink.databend.table-prefix", defaultValue = "debezium")
    String tablePrefix;
    public static SparkSession spark = SparkSession.builder().appName("unittest").master("local[2]").getOrCreate();

    public static void PGCreateTestDataTable() throws Exception {
        String sql = "" +
                "        CREATE TABLE IF NOT EXISTS inventory.test_data (\n" +
                "            c_id INTEGER ,\n" +
                "            c_text TEXT,\n" +
                "            c_varchar VARCHAR" +
                "          );";
        SourcePostgresqlDB.runSQL(sql);
    }

    public static ResultSet select(String sql)
            throws SQLException, ClassNotFoundException {
        Statement statement = (new TargetDatabendDB()).createConnection().createStatement();
        DatabendResultSet resultSet;
        try {
            resultSet = (DatabendResultSet) statement.executeQuery(sql);
        } catch (Throwable e) {
            try {
                statement.close();
            } catch (Throwable closeException) {
                if (closeException != e) {
                    e.addSuppressed(closeException);
                }
            }

            throw e;
        }
        return resultSet;
    }

    public static ResultSet getDatabendTableData(String sql) throws SQLException, ClassNotFoundException {
        return select(sql);
    }

    public static int getResultSetRowCount(ResultSet rs) throws SQLException {
        int num = 0;
        while (rs.next()) {
            num += 1;
        }
        return num;
    }


    public static int PGLoadTestDataTable(int numRows) {
        return PGLoadTestDataTable(numRows, false);
    }

    public static int PGLoadTestDataTable(int numRows, boolean addRandomDelay) {
        int numInsert = 0;
        do {

            new Thread(() -> {
                try {
                    if (addRandomDelay) {
                        Thread.sleep(TestUtil.randomInt(20000, 100000));
                    }
                    String sql = "INSERT INTO inventory.test_data (c_id, c_text, c_varchar ) " +
                            "VALUES ";
                    StringBuilder values = new StringBuilder("\n(" + TestUtil.randomInt(15, 32) + ", '" + TestUtil.randomString(524) + "', '" + TestUtil.randomString(524) + "')");
                    for (int i = 0; i < 100; i++) {
                        values.append("\n,(").append(TestUtil.randomInt(15, 32)).append(", '").append(TestUtil.randomString(524)).append("', '").append(TestUtil.randomString(524)).append("')");
                    }
                    SourcePostgresqlDB.runSQL(sql + values);
                    SourcePostgresqlDB.runSQL("COMMIT;");
                } catch (Exception e) {
                    Thread.currentThread().interrupt();
                }
            }).start();

            numInsert += 100;
        } while (numInsert <= numRows);
        return numInsert;
    }

    public static void mysqlCreateTestDataTable() throws Exception {
        String sql = "\n" +
                "        CREATE TABLE IF NOT EXISTS inventory.test_data (\n" +
                "            c_id INTEGER ,\n" +
                "            c_text TEXT,\n" +
                "            c_varchar TEXT\n" +
                "          );";
        SourceMysqlDB.runSQL(sql);
    }

    public static int mysqlLoadTestDataTable(int numRows) throws Exception {
        int numInsert = 0;
        do {
            String sql = "INSERT INTO inventory.test_data (c_id, c_text, c_varchar ) " +
                    "VALUES ";
            StringBuilder values = new StringBuilder("\n(" + TestUtil.randomInt(15, 32) + ", '" + TestUtil.randomString(524) + "', '" + TestUtil.randomString(524) + "')");
            for (int i = 0; i < 10; i++) {
                values.append("\n,(").append(TestUtil.randomInt(15, 32)).append(", '").append(TestUtil.randomString(524)).append("', '").append(TestUtil.randomString(524)).append("')");
            }
            SourceMysqlDB.runSQL(sql + values);
            numInsert += 10;
        } while (numInsert <= numRows);
        return numInsert;
    }
}
