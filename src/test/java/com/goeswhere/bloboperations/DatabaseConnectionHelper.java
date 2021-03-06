package com.goeswhere.bloboperations;

import org.junit.BeforeClass;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;

public class DatabaseConnectionHelper {
    static DataSource ds;
    static JdbcTemplate jdbc;
    static TransactionTemplate transactions;

    @BeforeClass
    public static void connect() {
        ds = new DriverManagerDataSource("jdbc:postgresql:test", "test", "test");
        jdbc = new JdbcTemplate(ds);
        transactions = new TransactionTemplate(new DataSourceTransactionManager(ds));
        jdbc.execute("SELECT 1");
        try {
            jdbc.execute("TRUNCATE TABLE blopstest.blob");
            jdbc.execute("TRUNCATE TABLE blopstest.metadata");
        } catch (DataAccessException e) {
            throw new IllegalStateException("couldn't find tables, please create them using create.psql", e);
        }
    }

}
