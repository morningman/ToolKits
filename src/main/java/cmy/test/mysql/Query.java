package cmy.test.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Query {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 3306;
    private static final String DB = "db1";
    private static final String USER = "root";
    private static final String PASSWD = "123456";
    private static final String DB_URL_PATTERN = "jdbc:mysql://%s:%d/%s";

    private static final String QUERY = "select 'abc' from t1";

    public static void main(String[] args) {
        try {
            ExecutionResultSet resultSet = query();
            System.out.println(resultSet.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static ExecutionResultSet query() throws ClassNotFoundException, SQLException {
        Connection conn = null;
        Statement stmt = null;
        String dbUrl = String.format(DB_URL_PATTERN, HOST, PORT, DB);
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(dbUrl, USER, PASSWD);
            long startTime = System.currentTimeMillis();
            stmt = conn.prepareStatement(QUERY, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            // set fetch size to MIN_VALUE to enable streaming result set to avoid OOM.
            ((PreparedStatement) stmt).setFetchSize(Integer.MIN_VALUE);
            ResultSet rs = ((PreparedStatement) stmt).executeQuery();
            ExecutionResultSet resultSet = generateResultSet(rs, startTime);
            rs.close();
            return resultSet;
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException se2) {
                se2.printStackTrace();
            }
            try {
                if (conn != null) conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
    }

    private static ExecutionResultSet generateResultSet(ResultSet rs, long startTime) throws SQLException {
        Map<String, Object> result = new HashMap<String, Object>();
        result.put("type", "query_result");
        if (rs == null) {
            return new ExecutionResultSet(result);
        }
        ResultSetMetaData metaData = rs.getMetaData();
        int colNum = metaData.getColumnCount();
        // 1. metadata
        List<Map<String, String>> metaFields = new ArrayList<Map<String, String>>();
        // index start from 1
        for (int i = 1; i <= colNum; ++i) {
            Map<String, String> field = new HashMap<String, String>();
            field.put("name", metaData.getColumnName(i));
            field.put("type", metaData.getColumnTypeName(i));
            metaFields.add(field);
        }
        // 2. data
        List<List<Object>> rows = new ArrayList<List<Object>>();
        while (rs.next()) {
            List<Object> row = new ArrayList<Object>(colNum);
            // index start from 1
            for (int i = 1; i <= colNum; ++i) {
                row.add(rs.getObject(i));
            }
            rows.add(row);
        }
        result.put("meta", metaFields);
        result.put("data", rows);
        result.put("time", (System.currentTimeMillis() - startTime));
        return new ExecutionResultSet(result);
    }

    public static class ExecutionResultSet {
        private Map<String, Object> result;

        public ExecutionResultSet(Map<String, Object> result) {
            this.result = result;
        }

        public void setResult(Map<String, Object> result) {
            this.result = result;
        }

        public Map<String, Object> getResult() {
            return result;
        }

        public static ExecutionResultSet emptyResult() {
            Map<String, Object> result = new HashMap<String, Object>();
            result.put("meta", new ArrayList<String>());
            result.put("data", new ArrayList<String>());
            return new ExecutionResultSet(result);
        }

        @Override
        public String toString() {
            return result.toString();
        }
    }

}
