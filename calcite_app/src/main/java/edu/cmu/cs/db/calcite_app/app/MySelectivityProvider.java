package edu.cmu.cs.db.calcite_app.app;

import javax.annotation.Nullable;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

public class MySelectivityProvider extends RelMdSelectivity {
    
    @Override
    public @Nullable Double getSelectivity(TableScan scan, RelMetadataQuery mq, @Nullable RexNode predicate) {
        double sel = 1.0;
        if ((predicate == null) || predicate.isAlwaysTrue()) {
            return sel;
        }

        DuckDBTable table = scan.getTable().unwrapOrThrow(DuckDBTable.class);
        double artificialSel = 1.0;

        for (RexNode pred : RelOptUtil.conjunctions(predicate)) {
            if (pred.isA(SqlKind.EQUALS)) {
                RexNode left = ((RexCall) predicate).getOperands().get(0);
                RexNode right = ((RexCall) predicate).getOperands().get(1);
                RexInputRef col = null;
                if (left instanceof RexInputRef) {
                    col = (RexInputRef) left;
                } else if (right instanceof RexInputRef) {
                    col = (RexInputRef) right;
                }
                if (col != null) {
                    try {
                        sel *= 1.0/table.getColumns().get(col.getIndex()).ndv;
                        continue;
                    } catch (Exception e) { }
                }
                sel *= .15;
            } else if (pred.getKind() == SqlKind.IS_NOT_NULL) {
                sel *= .9;
            } else if ((pred instanceof RexCall)
                    && (((RexCall) pred).getOperator() == RelMdUtil.ARTIFICIAL_SELECTIVITY_FUNC)) {
                artificialSel *= RelMdUtil.getSelectivityValue(pred);
            } else if (pred.isA(SqlKind.COMPARISON)) {
                sel *= .5;
            } else {
                sel *= .25;
            }
        }

        return sel * artificialSel;
    }
}