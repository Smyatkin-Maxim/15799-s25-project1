package edu.cmu.cs.db.calcite_app.app;

import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

/**
 * Planner rule that pulls up common factors from conditions,
 * e.g. (a and b) or (a and c) --> a and (b or c)
 */
@Value.Enclosing
public class FilterReorderRule extends RelRule<FilterReorderRule.Config>
        implements SubstitutionRule {

    /** Creates a FilterMergeRule. */
    protected FilterReorderRule(Config config) {
        super(config);
    }

    // ~ Methods ----------------------------------------------------------------

    @Override
    public boolean matches(final RelOptRuleCall call) {
        final Filter filter = call.rel(0);
        return !reorderCondition(filter, filter.getCondition(), call.getMetadataQuery(), filter.getCluster().getRexBuilder()).toString().
            equals(filter.getCondition().toString());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Filter filter = call.rel(0);
        final RelMetadataQuery mq = call.getMetadataQuery();
        final RelBuilder relBuilder = call.builder();
        final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        RexNode newCondition = reorderCondition(filter, filter.getCondition(), mq, rexBuilder);
        System.out.println("Was " + filter.getCondition().toString());
        System.out.println("Now " + newCondition.toString());
        relBuilder.push(filter.getInput()).filter(newCondition);
        call.transformTo(relBuilder.build());
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableFilterReorderRule.Config.builder()
                .operandSupplier(b0 -> b0.operand(LogicalFilter.class).anyInputs())
                .build();

        @Override
        default FilterReorderRule toRule() {
            return new FilterReorderRule(this);
        }

        /** Defines an operand tree for the given classes. */
        default Config withOperandFor(Class<? extends Filter> filterClass) {
            return withOperandSupplier(
                    b0 -> b0.operand(filterClass).anyInputs())
                    .as(Config.class);
        }
    }

    private class SelectivityComparator implements Comparator<RexNode> {
        private final RelMetadataQuery mq;
        private final Filter filter;

        SelectivityComparator(RelMetadataQuery mq, Filter filter) {
            this.mq = mq;
            this.filter = filter;
        }

        @Override
        public int compare(RexNode c1, RexNode c2) {
            Double s1 = mq.getSelectivity(filter.getInput(), c1);
            Double s2 = mq.getSelectivity(filter.getInput(), c2);
            if (s1 == null)
                s1 = 0.3;
            if (s2 == null)
                s2 = 0.3;
            return s1 == s2 ? 0 : (s1 < s2 ? -1 : 1);
        }
    }

    private RexNode reorderCondition(Filter filter, RexNode condition, RelMetadataQuery mq, RexBuilder rexBuilder) {
        List<RexNode> predicates = RelOptUtil.conjunctions(condition);
        if (predicates.size() < 2) {
            return condition;
        }
        ListIterator<RexNode> iterator = predicates.listIterator();
        while (iterator.hasNext()) {
            iterator.set(reorderCondition(filter, iterator.next(), mq, rexBuilder));
        }
        predicates.sort(new SelectivityComparator(mq, filter));
        return RexUtil.composeConjunction(rexBuilder, predicates, false);
    }
}
