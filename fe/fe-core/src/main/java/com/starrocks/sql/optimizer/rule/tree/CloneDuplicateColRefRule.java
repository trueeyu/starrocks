// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.optimizer.rule.tree;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CloneOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.List;
import java.util.Map;

// ProjectOperator or Projection of Operator may have several ColumnRefs remapped to the same ColumnRef, for an example:
// 1.ColumnRef(1)->ColumnRef(1);
// 2.ColumnRef(2)->ColumnRef(1);
// This would lead to that column shared by multiple SlotRefs in a Chunk during the plan executed in BE,
// when some conjuncts apply to such chunks, the shared column may be written twice unexpectedly; at present,
// BE does not support COW; so we substitute duplicate ColumnRef with CloneOperator to avoid this.
// After this Rule applied, the ColumnRef remapping will convert to:
// 1.ColumnRef(1)->ColumnRef(1);
// 2.ColumnRef(2)->CloneOperator(ColumnRef(1)).
public class CloneDuplicateColRefRule implements TreeRewriteRule {
    private static final Visitor VISITOR = new Visitor();

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        VISITOR.visit(root, null);
        return root;
    }

    private static class Visitor extends OptExpressionVisitor<Void, Void> {
        void substColumnRefOperatorWithCloneOperator(Map<ColumnRefOperator, ScalarOperator> colRefMap) {
            if (colRefMap == null || colRefMap.isEmpty()) {
                return;
            }

            Map<ScalarOperator, Integer> duplicateColRefs = Maps.newHashMap();
            colRefMap.forEach((k, v) -> {
                if (!v.isColumnRef()) {
                    return;
                }
                duplicateColRefs.put(v, duplicateColRefs.getOrDefault(v, 0) + 1);
            });

            for (ColumnRefOperator key : colRefMap.keySet()) {
                ScalarOperator value = colRefMap.get(key);
                if (value.isColumnRef() && duplicateColRefs.get(value) > 1 && !key.equals(value)) {
                    duplicateColRefs.put(value, duplicateColRefs.get(value) - 1);
                    colRefMap.put(key, new CloneOperator(value));
                }
            }
        }

        // PhysicalCTEConsumeOperator carries its consumer->producer mapping in cteOutputColumnRefMap, a separate
        // Map<ColumnRefOperator, ColumnRefOperator> that is later materialized into a ProjectNode at plan-fragment
        // build time. Its value type does not admit a CloneOperator, so duplicates are resolved by dedup'ing the map
        // (keeping one consumer ref per producer ref) and re-emitting the demoted refs as CloneOperator(canonical)
        // via the operator's Projection, which PlanFragmentBuilder.visit layers on top of the consume ProjectNode.
        void substCteConsumeDuplicates(PhysicalCTEConsumeOperator consume) {
            Map<ColumnRefOperator, ColumnRefOperator> cteMap = consume.getCteOutputColumnRefMap();
            if (cteMap == null || cteMap.size() < 2) {
                return;
            }

            Map<ColumnRefOperator, List<ColumnRefOperator>> byProducer = Maps.newHashMap();
            cteMap.forEach((consumerRef, producerRef) ->
                    byProducer.computeIfAbsent(producerRef, k -> Lists.newArrayList()).add(consumerRef));

            if (byProducer.values().stream().noneMatch(l -> l.size() > 1)) {
                return;
            }

            // The consume's local predicate runs directly on the consume ProjectNode, so any ref it references must
            // survive in cteOutputColumnRefMap. Prefer predicate-referenced refs as canonical. If more than one such
            // ref shares a producer ref, bail out to stay safe.
            ColumnRefSet predicateCols = consume.getPredicate() != null
                    ? consume.getPredicate().getUsedColumns() : new ColumnRefSet();

            Map<ColumnRefOperator, ScalarOperator> replaceMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, List<ColumnRefOperator>> entry : byProducer.entrySet()) {
                List<ColumnRefOperator> consumerRefs = entry.getValue();
                if (consumerRefs.size() < 2) {
                    continue;
                }
                long predUsesInGroup = consumerRefs.stream().filter(r -> predicateCols.contains(r.getId())).count();
                if (predUsesInGroup > 1) {
                    return;
                }
                ColumnRefOperator canonical = consumerRefs.stream()
                        .filter(r -> predicateCols.contains(r.getId()))
                        .findFirst()
                        .orElse(consumerRefs.get(0));
                for (ColumnRefOperator consumerRef : consumerRefs) {
                    if (!consumerRef.equals(canonical)) {
                        replaceMap.put(consumerRef, new CloneOperator(canonical));
                    }
                }
            }

            if (replaceMap.isEmpty()) {
                return;
            }

            replaceMap.keySet().forEach(cteMap::remove);

            Projection existing = consume.getProjection();
            if (existing == null) {
                Map<ColumnRefOperator, ScalarOperator> projMap = Maps.newHashMap();
                cteMap.keySet().forEach(ref -> projMap.put(ref, ref));
                projMap.putAll(replaceMap);
                consume.setProjection(new Projection(projMap));
            } else {
                // Substitute the demoted refs anywhere they appear inside the existing projection. The projection
                // is re-emitted on top of the (now dedup'd) consume ProjectNode, so the demoted refs must be
                // replaced with CloneOperator(canonical) before they are evaluated.
                ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(replaceMap);
                existing.getColumnRefMap().replaceAll((k, v) -> rewriter.rewrite(v));
                existing.getCommonSubOperatorMap().replaceAll((k, v) -> rewriter.rewrite(v));
            }
        }

        @Override
        public Void visit(OptExpression optExpression, Void context) {
            Operator op = optExpression.getOp();
            if (op instanceof PhysicalProjectOperator) {
                PhysicalProjectOperator projectOp = op.cast();
                substColumnRefOperatorWithCloneOperator(projectOp.getColumnRefMap());
                substColumnRefOperatorWithCloneOperator(projectOp.getCommonSubOperatorMap());
            } else {
                if (op instanceof PhysicalCTEConsumeOperator) {
                    substCteConsumeDuplicates(op.cast());
                }
                if (op.getProjection() != null) {
                    Projection projection = op.getProjection();
                    substColumnRefOperatorWithCloneOperator(projection.getColumnRefMap());
                    substColumnRefOperatorWithCloneOperator(projection.getCommonSubOperatorMap());
                }
            }
            optExpression.getInputs().forEach(input -> this.visit(input, context));
            return null;
        }
    }
}
