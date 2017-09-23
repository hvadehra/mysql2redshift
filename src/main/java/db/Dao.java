package db;

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

/**
 * Created by hemanshu.v on 23/09/17.
 */
public abstract class Dao {

    private static int poolSize = 1;
    private final JdbcTemplate db;
    private String SELECT_WITHOUT_LIMIT = "SELECT %s FROM %s";
    private String SELECT_WITH_ORDER_AND_LIMIT = "SELECT %s FROM %s ORDER BY %s LIMIT %d OFFSET %d";

    Dao(String url, String driver, int timeout) {
        this.db = getJdbcTemplate(createDataSource(url, driver, timeout));
    }

    public void readAllRows(String table, List<String> columns, RowCallbackHandler callback) {
        String columnsStr = columns.stream()
                .reduce((s, s2) -> s + "," + s2).orElse("*");
        String sql = String.format(SELECT_WITHOUT_LIMIT, columnsStr, table);
        db.query(sql, callback);
    }

    public List<List<Object>> readChunk(String table, List<String> columns, List<String> keys, int limit, int offset) {
        String orderingKeys = keys.stream()
                .reduce("", (s1, s2) -> s1 + s2);
        String columnsStr = columns.stream()
                .reduce((s, s2) -> s + "," + s2).orElse("*");
        String sql = String.format(SELECT_WITH_ORDER_AND_LIMIT, columnsStr, table, orderingKeys, limit, offset);
        System.out.println("Executing SQL: " + sql);
        return db.query(sql, getListRowMapper(columns));
    }

    public static RowMapper<List<Object>> getListRowMapper(List<String> columns) {
        return (rs, i) -> columns.stream()
                .map(col -> {
                    try {
                        return rs.getObject(col);
                    } catch (SQLException e) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                    return null;
                }).collect(Collectors.toList());
    }

    public List<String> getPrimaryKeys(String dbName, String table) {
        String sql = "SELECT k.column_name FROM information_schema.table_constraints t JOIN information_schema.key_column_usage k USING(constraint_name,table_schema,table_name) WHERE t.constraint_type='PRIMARY KEY'   AND t.table_schema='" + dbName + "'  AND t.table_name='" + table + "' order by k.ordinal_position;";
        return db.query(sql, (rs, i) -> rs.getString("column_name"));
    }

    private static JdbcTemplate getJdbcTemplate(BasicDataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    private static BasicDataSource createDataSource(String url, String driver, long timeout) {
        BasicDataSource dataSource = new BasicDataSource();

        dataSource.setMaxActive(poolSize);
        dataSource.setMaxWait(TimeUnit.SECONDS.toMillis(timeout));
        dataSource.setPoolPreparedStatements(true);
        dataSource.setValidationQuery("select 1");

        dataSource.setDriverClassName(driver);
        dataSource.setUrl(url);
        dataSource.setUsername(System.getenv("SHIFT_DB_USERNAME"));
        dataSource.setPassword(System.getenv("SHIFT_DB_PASSWORD"));
        return dataSource;

    }

    public List<String> getColumns(String dbName, String table) {
        String sql = "select column_name from information_schema.columns where table_name = '" + table +
                "' and table_schema = '" + dbName + "' order by ordinal_position";
        return db.query(sql, (rs, i) -> rs.getString("column_name"));
    }
}
