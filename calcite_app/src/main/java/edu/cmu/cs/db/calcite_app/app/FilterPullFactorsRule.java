/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

/**
 * Planner rule that combines two
 * {@link org.apache.calcite.rel.logical.LogicalFilter}s.
 */
@Value.Enclosing
public class FilterPullFactorsRule extends RelRule<FilterPullFactorsRule.Config>
        implements SubstitutionRule {

    /** Creates a FilterMergeRule. */
    protected FilterPullFactorsRule(Config config) {
        super(config);
    }

    // ~ Methods ----------------------------------------------------------------

    @Override
    public boolean matches(final RelOptRuleCall call) {
        final Filter filter = call.rel(0);
        return !RexUtil.pullFactors(filter.getCluster().getRexBuilder(), filter.getCondition())
                .equals(filter.getCondition());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Filter filter = call.rel(0);
        final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();

        RexNode newCondition = RexUtil.pullFactors(rexBuilder, filter.getCondition());
        final RelBuilder relBuilder = call.builder();
        relBuilder.push(filter.getInput()).filter(newCondition);
        call.transformTo(relBuilder.build());
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableFilterPullFactorsRule.Config.builder()
                .operandSupplier(b0 -> b0.operand(LogicalFilter.class).anyInputs())
                .build();

        @Override
        default FilterPullFactorsRule toRule() {
            return new FilterPullFactorsRule(this);
        }

        /** Defines an operand tree for the given classes. */
        default Config withOperandFor(Class<? extends Filter> filterClass) {
            return withOperandSupplier(
                    b0 -> b0.operand(filterClass).anyInputs())
                    .as(Config.class);
        }
    }
}
