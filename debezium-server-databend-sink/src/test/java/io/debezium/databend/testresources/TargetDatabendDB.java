package io.debezium.databend.testresources;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TargetDatabendDB implements QuarkusTestResourceLifecycleManager {
    public static final String DB_USER = "databend";
    public static final String DB_PASSWORD = "databend";
    public String DB_DATABASE = "public";
    private static final Logger LOGGER = LoggerFactory.getLogger(TargetDatabendDB.class);

    public Connection createConnection()
            throws SQLException, ClassNotFoundException {
        String url = "jdbc:databend://localhost:8000";
        Class.forName("com.databend.jdbc.DatabendDriver");
        return DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
    }

    public static void runSQL(String query) throws SQLException, ClassNotFoundException {
        try {
            String url = "jdbc:databend://localhost:8000";
            Class.forName("com.databend.jdbc.DatabendDriver");
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
        Map<String, String> config = new ConcurrentHashMap<>();
        config.put("debezium.sink.databend.database.url", "jdbc:databend://localhost:8000");
        config.put("debezium.sink.databend.database.username", "databend");
        config.put("debezium.sink.databend.database.password", "databend");
        config.put("debezium.sink.databend.database.databaseName", "public");
        config.put("debezium.sink.databend.database.param.xyz", "val");
        return config;
    }

    @Override
    public void stop() {

    }
}
