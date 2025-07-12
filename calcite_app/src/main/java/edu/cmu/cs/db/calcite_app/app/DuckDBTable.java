package edu.cmu.cs.db.calcite_app.app;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DuckDBTable extends AbstractTable
        implements ScannableTable/* , ProjectableFilterableTable */ {

    public class Column {
        public String name;
        public int type;
        public String typeName;
        public boolean unique;
        public int id;

        Column(String name, int type, String typeName, boolean unique, int id) {
            this.name = name;
            this.type = type;
            this.typeName = typeName;
            this.unique = unique;
            this.id = id;
        }
    }

    private Connection conn;
    private String name;
    private Object[][] data;
    private List<Column> columns;
    private RelDataType rowType;

    DuckDBTable(Connection conn, String name) throws Exception {
        this.conn = conn;
        this.name = name;

        getColumns();
        materialize();
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (rowType == null) {
            final List<RelDataType> types = new ArrayList<>();
            final List<String> names = new ArrayList<>();
            for (Column c : columns) {
                names.add(c.name);
                types.add(typeFactory.createSqlType(SqlTypeName.getNameForJdbcType(c.type)));
            }
            rowType = typeFactory.createStructType(Pair.zip(names, types));
        }
        return rowType;
    }

    @Override
    public Enumerable<@Nullable Object[]> scan(DataContext root) {
        return Linq4j.asEnumerable(this.data);
    }

    /*
     * @Override
     * public Enumerable<@Nullable Object[]> scan(DataContext root, List<RexNode>
     * filters, int @Nullable [] projects) {
     * return new DuckDBProjectableEnumerable(projects, data);
     * }
     */

    @Override
    public Statistic getStatistic() {
        final List<ImmutableBitSet> keys = new ArrayList<>();
        // NOTE: was hoping that Calcite can use this information for better plans
        // but instead it generates bogus SQL, at least in duckdb opinion
        for (Column c : columns) {
            if (c.unique) {
                keys.add(ImmutableBitSet.of(c.id));
            }
        }
        return Statistics.of(cardinality(), keys, new ArrayList<>());
    }

    public List<Column> getColumns() throws Exception {
        if (columns == null) {
            ResultSet rs = conn.getMetaData().getColumns(null, null, name, null);
            columns = new ArrayList<Column>();
            while (rs.next()) {
                Boolean isUnique = nDistinct(rs.getString("COLUMN_NAME")) == cardinality();
                columns.add(
                        new Column(rs.getString("COLUMN_NAME"), rs.getInt("DATA_TYPE"), rs.getString("TYPE_NAME"),
                                isUnique, rs.getInt("ORDINAL_POSITION")));
            }
            Collections.sort(columns, (c1, c2) -> c1.id - c2.id);
        }
        return columns;
    }

    public int cardinality() {
        if (data != null) {
            return data.length;
        }
        try {
            ResultSet rs = conn.prepareStatement("select count(*) from " + name).executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (Exception e) {
            System.err.println("Can't fetch cardinality of " + name + ". " + e.getMessage());
            return 100;
        }
    }

    private int nDistinct(String column) throws Exception {

        ResultSet rs = conn.prepareStatement("select count(distinct \"" + column +
                "\") from \"" + name + "\"")
                .executeQuery();
        rs.next();
        return rs.getInt(1);
    }

    private Object[][] materialize() throws Exception {
        System.out.println("Materializing " + name);
        data = new Object[cardinality()][columns.size()];
        PreparedStatement ps = conn.prepareStatement("select * from " + name);
        ResultSet rs = ps.executeQuery();
        int rowId = 0;
        while (rs.next()) {
            for (int i = 0; i < columns.size(); i++) {
                data[rowId][i] = rs.getObject(i + 1);
                if (data[rowId][i] instanceof java.time.LocalDate) {
                    data[rowId][i] = Date.valueOf((java.time.LocalDate) data[rowId][i]);
                }
            }
            rowId++;
        }
        rs.close();
        ps.close();
        return data;
    }
}
