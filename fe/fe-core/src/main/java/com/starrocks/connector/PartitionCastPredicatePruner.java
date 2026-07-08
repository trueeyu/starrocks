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

package com.starrocks.connector;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.FoldConstantsRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * StarRocks-side residual partition pruning for STRING partition columns compared against a temporal value.
 *
 * <p>When a query compares a STRING partition column with a DATE/DATETIME value, binary-predicate coercion
 * wraps the column in a cast, e.g. {@code CAST(c AS DATETIME) = '2020-06-14 00:00:00'}. The backend evaluates
 * this in the DATETIME domain (parses the string then compares). Several connectors' native predicate
 * pushdown, however, only compare the column in its declared STRING type: they unwrap the cast and render the
 * temporal constant back to a string ({@code '2020-06-14 00:00:00'}), which never equals a {@code 'yyyy-MM-dd'}
 * partition value, so every data file is pruned and the query wrongly returns empty.
 *
 * <p>To keep pruning consistent with the backend filter, such conjuncts are kept out of the pushed predicate
 * and evaluated here against each file's partition values, reusing StarRocks' own {@code CAST(VARCHAR AS
 * DATETIME)} folding (identical parsing semantics to the backend). A file is dropped only when a residual
 * conjunct definitively folds to {@code false}; anything indeterminate (unfoldable, parse failure, non-boolean)
 * keeps the file, so the pruning can only ever be more conservative than the backend filter (never unsound).
 *
 * <p>The connector supplies the identity string partition column names and, per file, the raw string partition
 * values; the split/evaluation logic here is connector agnostic.
 */
public class PartitionCastPredicatePruner {
    private static final Logger LOG = LogManager.getLogger(PartitionCastPredicatePruner.class);

    private PartitionCastPredicatePruner() {
    }

    /**
     * A conjunct is a "cast residual" iff every column it references is an identity string partition column AND
     * at least one of them is wrapped in a cast to a temporal type (the case native pushdown prunes unsoundly).
     * Conjuncts referencing any other column, or with no such cast, are left to the connector / backend.
     */
    public static boolean isCastResidual(ScalarOperator conjunct, Set<String> identityStringPartitionColumns) {
        List<ColumnRefOperator> refs = conjunct.getColumnRefs();
        if (refs.isEmpty()) {
            return false;
        }
        for (ColumnRefOperator ref : refs) {
            if (!identityStringPartitionColumns.contains(ref.getName().toLowerCase(Locale.ROOT))) {
                return false;
            }
        }
        return hasTemporalCastOnColumn(conjunct);
    }

    private static boolean hasTemporalCastOnColumn(ScalarOperator operator) {
        if (operator instanceof CastOperator) {
            ScalarOperator child = operator.getChild(0);
            if (child instanceof ColumnRefOperator
                    && (operator.getType().isDate() || operator.getType().isDatetime())) {
                return true;
            }
        }
        for (ScalarOperator child : operator.getChildren()) {
            if (hasTemporalCastOnColumn(child)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Splits conjuncts into those pushable to the connector's native predicate and the cast residual conjuncts
     * to evaluate here. {@code identityStringPartitionColumns} must be lower-cased column names.
     */
    public static PartitionResidual split(List<ScalarOperator> conjuncts, Set<String> identityStringPartitionColumns) {
        List<ScalarOperator> pushable = Lists.newArrayList();
        List<ScalarOperator> residual = Lists.newArrayList();
        for (ScalarOperator conjunct : conjuncts) {
            if (!identityStringPartitionColumns.isEmpty()
                    && isCastResidual(conjunct, identityStringPartitionColumns)) {
                residual.add(conjunct);
            } else {
                pushable.add(conjunct);
            }
        }
        return new PartitionResidual(pushable, residual);
    }

    /**
     * Returns {@code true} if the partition (given as column name -> raw string value) may satisfy the residual
     * conjuncts, i.e. no conjunct definitively evaluates to {@code false}. Any conjunct that cannot be folded to
     * a boolean constant (parse failure, unbound reference, etc.) is treated as a possible match.
     *
     * @param partitionValues column name (any case) -> raw partition string value.
     */
    public static boolean partitionMayMatch(List<ScalarOperator> residualConjuncts,
                                            Map<String, String> partitionValues) {
        if (residualConjuncts.isEmpty()) {
            return true;
        }

        Map<String, String> lowerValues = new HashMap<>();
        for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
            if (entry.getValue() != null) {
                lowerValues.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue());
            }
        }

        for (ScalarOperator conjunct : residualConjuncts) {
            Map<ColumnRefOperator, ScalarOperator> replaceMap = new HashMap<>();
            boolean allBound = true;
            for (ColumnRefOperator ref : conjunct.getColumnRefs()) {
                String value = lowerValues.get(ref.getName().toLowerCase(Locale.ROOT));
                if (value == null) {
                    allBound = false;
                    break;
                }
                replaceMap.put(ref, ConstantOperator.createVarchar(value));
            }
            if (!allBound) {
                // Cannot bind every reference to a partition value; be conservative and keep the file.
                continue;
            }

            Boolean result = tryFoldToBoolean(conjunct, replaceMap);
            if (result != null && !result) {
                return false;
            }
        }
        return true;
    }

    private static Boolean tryFoldToBoolean(ScalarOperator conjunct,
                                            Map<ColumnRefOperator, ScalarOperator> replaceMap) {
        try {
            ScalarOperator replaced = new ReplaceColumnRefRewriter(replaceMap).rewrite(conjunct);
            ScalarOperator folded = new ScalarOperatorRewriter().rewrite(
                    replaced, Lists.newArrayList(new FoldConstantsRule()));
            if (folded instanceof ConstantOperator) {
                ConstantOperator constant = (ConstantOperator) folded;
                if (constant.isNull()) {
                    // NULL predicate = not true -> this partition value does not match.
                    return false;
                }
                if (constant.getType().isBoolean()) {
                    return constant.getBoolean();
                }
            }
        } catch (Exception e) {
            // Strict cast parse failure or any other folding issue: fall back to keeping the file.
            LOG.debug("residual partition predicate not foldable, keep file: {}", conjunct, e);
        }
        return null;
    }

    /** Result of splitting conjuncts for partition pruning. */
    public static class PartitionResidual {
        public final List<ScalarOperator> pushable;
        public final List<ScalarOperator> residual;

        public PartitionResidual(List<ScalarOperator> pushable, List<ScalarOperator> residual) {
            this.pushable = pushable;
            this.residual = residual;
        }

        public boolean hasResidual() {
            return !residual.isEmpty();
        }
    }
}