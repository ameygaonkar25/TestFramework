package com.bugbuster.utils;

import java.sql.*;

public class DBUtils {

    String DB_URL = "jdbc:XXXX:XXXX@XXX.XXX.XXX.XXX:XXXX:orcl";
    String DB_USERNAME = "system";
    String DB_PASSWORD = "XXXXXX";
    Connection connection;

    public DBUtils() throws SQLException{
        this.connection = DriverManager.getConnection(
                DB_URL,
                DB_USERNAME,
                DB_PASSWORD);
    }

    public void truncateExecutorTables() throws Exception {
        String truncateExecutorConfig = "TRUNCATE TABLE mozart_job_executor_config";
        String truncateGroupConfig = "TRUNCATE TABLE mozart_job_executor_group_config";

        try (PreparedStatement executorStatement =
                     connection.preparedStatement(truncateExecutorConfig);

             PreparedStatement groupStatement =
                     connection.preparedStatement(truncateGroupConfig)) {

            executorStatement.executeUpdate();
            groupStatement.executeUpdate();

            System.out.println("Successfully truncate executor tables");

        }
    }
}
