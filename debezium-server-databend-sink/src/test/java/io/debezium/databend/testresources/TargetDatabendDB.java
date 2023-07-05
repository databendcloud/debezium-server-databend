package io.debezium.databend.testresources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TargetDatabendDB {
    public static final String DB_USER = "databend";
    public static final String DB_PASSWORD = "databend";
    public  String DB_DATABASE = "public";
    private static final Logger LOGGER = LoggerFactory.getLogger(TargetDatabendDB.class);

    public Connection createConnection()
            throws SQLException, ClassNotFoundException {
        String url = "jdbc:databend://localhost:8000";
        Class.forName("com.databend.jdbc.DatabendDriver");
        return DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
    }

}
