package edu.cmu.cs.db.calcite_app.app;

import javax.annotation.Nullable;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

public class MyRowCountProvider extends RelMdRowCount {
    /**
     * This method is a dirty hack to work around two Calcite flaws
     * 1. Calcite doesn't understanrecognize that `BigTbl INNER SmallTbl` is better
     * than `SmallTbl INNER BigTbl`. It seems that to do it properly I need to
     * provide phisical (enumerable?) Join implementations with proper memory
     * and cpu costs. But it would require me to provide same costs at least for all
     * other join types. And this leads us to the second flaw.
     * 2. By default Calcite only cares about row counts. VolcanoCost supports
     * Rows, CPU and IO. But in never generates IO costs and never makes decisions
     * based on CPU costs. Meaning that I'd need a new Cost model.
     * 
     * Due to this complication I just created this hack: deprioritize plans with
     * big inner inputs by a factor of 5.
     */
    @Nullable
    @Override
    public Double getRowCount(Join rel, RelMetadataQuery mq) {
        RelNode left = rel.getLeft(), right = rel.getRight();
        Double realRowCount = RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition());
        if (rel.getJoinType() == JoinRelType.INNER && mq.getRowCount(left) < mq.getRowCount(right)) {
            realRowCount *= 5;
        }
        return realRowCount;
    }
}
