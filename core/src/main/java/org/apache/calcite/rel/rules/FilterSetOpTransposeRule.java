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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that pushes a {@link org.apache.calcite.rel.core.Filter}
 * past a {@link org.apache.calcite.rel.core.SetOp}.
 *
 * @see CoreRules#FILTER_SET_OP_TRANSPOSE
 */
@Value.Enclosing
public class FilterSetOpTransposeRule
    extends RelRule<FilterSetOpTransposeRule.Config>
    implements TransformationRule {

  /** Creates a FilterSetOpTransposeRule. */
  protected FilterSetOpTransposeRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public FilterSetOpTransposeRule(RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class));
  }

  @Deprecated // to  be removed before 2.0
  public FilterSetOpTransposeRule(RelFactories.FilterFactory filterFactory) {
    this(Config.DEFAULT
        .withRelBuilderFactory(RelBuilder.proto(Contexts.of(filterFactory)))
        .as(Config.class));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    Filter filterRel = call.rel(0);
    SetOp setOp = call.rel(1);

    RexNode condition = filterRel.getCondition();

    // create filters on top of each setop child, modifying the filter
    // condition to reference each setop child
    RexBuilder rexBuilder = filterRel.getCluster().getRexBuilder();
    final RelBuilder relBuilder = call.builder();
    List<RelDataTypeField> origFields =
        setOp.getRowType().getFieldList();
    int[] adjustments = new int[origFields.size()];
    final List<RelNode> newSetOpInputs = new ArrayList<>();
    for (RelNode input : setOp.getInputs()) {
      RexNode newCondition =
          condition.accept(
              new RelOptUtil.RexInputConverter(
                  rexBuilder,
                  origFields,
                  input.getRowType().getFieldList(),
                  adjustments));
      newSetOpInputs.add(relBuilder.push(input).filter(newCondition).build());
    }

    // create a new setop whose children are the filters created above
    SetOp newSetOp =
        setOp.copy(setOp.getTraitSet(), newSetOpInputs);

    call.transformTo(newSetOp);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableFilterSetOpTransposeRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(Filter.class).oneInput(b1 ->
                b1.operand(SetOp.class).anyInputs()));

    @Override default FilterSetOpTransposeRule toRule() {
      return new FilterSetOpTransposeRule(this);
    }
  }
}
