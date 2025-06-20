package edu.cmu.cs.db.calcite_app.app;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableMap;

public class DuckDBSchema extends AbstractSchema {
    private @Nullable Map<String, Table> tableMap;

    public DuckDBSchema() throws Exception {
        super();
        tableMap = createTableMap();
    }

    @Override protected Map<String, Table> getTableMap() {
        if (tableMap == null) {
            try {
                tableMap = createTableMap();
            } catch (Exception e) {
                return null;
            }
        }
        return tableMap;
    }

    private Map<String, Table> createTableMap() throws Exception {
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        Class.forName("org.duckdb.DuckDBDriver");
        Connection conn = DriverManager.getConnection("jdbc:duckdb:../items.db");
        for (String tableName : getTableNames(conn)) {
            Table table = new DuckDBTable(conn, tableName);
            builder.put(tableName, table);
        }
        conn.close();
        return builder.build();
    }

    private static List<String> getTableNames(Connection conn) throws Exception {
        ResultSet rs = conn.getMetaData().getTables(null, null, null, null);
        List<String> answer = new ArrayList<String>();
        while (rs.next()) {
            answer.add(rs.getString("TABLE_NAME"));
        }
        return answer;
    }
}
