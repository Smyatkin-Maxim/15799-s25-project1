package edu.cmu.cs.db.calcite_app.app;

import java.util.Iterator;

import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rel.metadata.RelMdDistinctRowCount;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NumberUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

public class MyNdvProvider extends RelMdDistinctRowCount {

    public MyNdvProvider() {
    }

    @Override
    public @Nullable Double getDistinctRowCount(
            TableScan scan, RelMetadataQuery mq,
            ImmutableBitSet groupKey, RexNode predicate) {
        try {
            DuckDBTable table = scan.getTable().unwrapOrThrow(DuckDBTable.class);

            double n_distinct = 1;
            if (groupKey.size() == 0) {
                n_distinct = table.cardinality();
            } else {
                Iterator<Integer> i = groupKey.iterator();
                while (i.hasNext()) {
                    int columnIndex = i.next();
                    n_distinct *= table.getColumns().get(columnIndex).ndv;
                }
                if (n_distinct > table.cardinality()) {
                    n_distinct = table.cardinality();
                }
            }
            Double predsel = mq.getSelectivity(scan, predicate);
            n_distinct = NumberUtil.multiply(n_distinct, predsel);
            return n_distinct;
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return null;
        }
    }
}