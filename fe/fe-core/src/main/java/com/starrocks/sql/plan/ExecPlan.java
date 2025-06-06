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

package com.starrocks.sql.plan;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.common.FeConstants;
import com.starrocks.common.IdGenerator;
import com.starrocks.common.util.ProfilingExecPlan;
import com.starrocks.planner.ExecGroup;
import com.starrocks.planner.HashJoinNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.Explain;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.thrift.TExplainLevel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ExecPlan {
    private final ConnectContext connectContext;
    private final List<String> colNames;
    private final List<ScanNode> scanNodes = new ArrayList<>();
    private final List<Expr> outputExprs = new ArrayList<>();
    private final DescriptorTable descTbl = new DescriptorTable();
    private final Map<ColumnRefOperator, Expr> colRefToExpr = new HashMap<>();
    private final ArrayList<PlanFragment> fragments = new ArrayList<>();
    private final Map<Integer, PlanFragment> cteProduceFragments = Maps.newHashMap();
    // splitProduceFragments and joinNodeMap is used for skew join
    private final Map<Integer, PlanFragment> splitProduceFragments = Maps.newHashMap();

    private final Map<PhysicalHashJoinOperator, HashJoinNode> joinNodeMap = new HashMap<>();

    private int planCount = 0;

    private final OptExpression physicalPlan;
    private final List<ColumnRefOperator> outputColumns;

    private final IdGenerator<PlanNodeId> nodeIdGenerator = PlanNodeId.createGenerator();
    private final IdGenerator<PlanFragmentId> fragmentIdGenerator = PlanFragmentId.createGenerator();
    private final Map<Integer, OptExpression> optExpressions = Maps.newHashMap();
    private List<ExecGroup> execGroups = new ArrayList<>();

    private volatile ProfilingExecPlan profilingPlan;
    private LogicalPlan logicalPlan;
    private ColumnRefFactory columnRefFactory;

    private List<Integer> collectExecStatsIds;

    private final boolean isShortCircuit;

    private long useBaseline = -1;

    @VisibleForTesting
    public ExecPlan() {
        connectContext = new ConnectContext();
        connectContext.setQueryId(new UUID(1, 2));
        colNames = new ArrayList<>();
        physicalPlan = null;
        outputColumns = new ArrayList<>();
        isShortCircuit = false;
    }

    public ExecPlan(ConnectContext connectContext, List<String> colNames,
                    OptExpression physicalPlan, List<ColumnRefOperator> outputColumns, boolean isShortCircuit) {
        this.connectContext = connectContext;
        this.colNames = colNames;
        this.physicalPlan = physicalPlan;
        this.outputColumns = outputColumns;
        this.isShortCircuit = isShortCircuit;
    }

    // for broker load plan
    public ExecPlan(ConnectContext connectContext, List<PlanFragment> fragments) {
        this.connectContext = connectContext;
        this.colNames = new ArrayList<>();
        this.physicalPlan = null;
        this.outputColumns = new ArrayList<>();
        this.fragments.addAll(fragments);
        this.isShortCircuit = false;
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }

    public List<ScanNode> getScanNodes() {
        return scanNodes;
    }

    public List<Expr> getOutputExprs() {
        return outputExprs;
    }

    public ArrayList<PlanFragment> getFragments() {
        return fragments;
    }

    public PlanFragment getTopFragment() {
        return fragments.get(0);
    }

    public DescriptorTable getDescTbl() {
        return descTbl;
    }

    public List<String> getColNames() {
        return colNames;
    }

    public PlanNodeId getNextNodeId() {
        return nodeIdGenerator.getNextId();
    }

    public PlanFragmentId getNextFragmentId() {
        return fragmentIdGenerator.getNextId();
    }

    public Map<ColumnRefOperator, Expr> getColRefToExpr() {
        return colRefToExpr;
    }

    public void setPlanCount(int planCount) {
        this.planCount = planCount;
    }

    public int getPlanCount() {
        return planCount;
    }

    public Map<Integer, PlanFragment> getCteProduceFragments() {
        return cteProduceFragments;
    }

    public Map<Integer, PlanFragment> getSplitProduceFragments() {
        return splitProduceFragments;
    }

    public Map<PhysicalHashJoinOperator, HashJoinNode> getJoinNodeMap() {
        return joinNodeMap;
    }

    public OptExpression getPhysicalPlan() {
        return physicalPlan;
    }

    public List<ColumnRefOperator> getOutputColumns() {
        return outputColumns;
    }

    public void setExecGroups(List<ExecGroup> execGroups) {
        this.execGroups = execGroups;
    }

    public List<ExecGroup> getExecGroups() {
        return this.execGroups;
    }

    public void setUseBaseline(long useBaseline) {
        this.useBaseline = useBaseline;
    }

    public void recordPlanNodeId2OptExpression(int id, OptExpression optExpression) {
        optExpression.getOp().setPlanNodeId(id);
        optExpressions.put(id, optExpression);
    }

    public static void assignOperatorIds(OptExpression root) {
        IdGenerator<PlanNodeId> operatorIdGenerator = PlanNodeId.createGenerator();
        assignOperatorIds(root, operatorIdGenerator);
    }

    private static void assignOperatorIds(OptExpression root, IdGenerator<PlanNodeId> operatorIdGenerator) {
        root.getOp().setOperatorId(operatorIdGenerator.getNextId().asInt());
        for (OptExpression child : root.getInputs()) {
            assignOperatorIds(child, operatorIdGenerator);
        }
    }

    public OptExpression getOptExpression(int planNodeId) {
        return optExpressions.get(planNodeId);
    }

    public ProfilingExecPlan getProfilingPlan() {
        if (profilingPlan == null) {
            synchronized (this) {
                if (profilingPlan == null) {
                    boolean needSetCtx = ConnectContext.get() == null && this.connectContext != null;
                    try {
                        if (needSetCtx) {
                            this.connectContext.setThreadLocalInfo();
                        }
                        profilingPlan = ProfilingExecPlan.buildFrom(this);
                    } finally {
                        if (needSetCtx) {
                            ConnectContext.remove();
                        }
                    }
                }
            }
        }
        return profilingPlan;
    }

    public String getExplainString(TExplainLevel level) {
        StringBuilder str = new StringBuilder();

        if (level == TExplainLevel.VERBOSE || level == TExplainLevel.COSTS) {
            if (FeConstants.showFragmentCost) {
                final String prefix = "  ";
                AuditEvent auditEvent = connectContext.getAuditEventBuilder().build();
                str.append("PLAN COST").append("\n")
                        .append(prefix).append("CPU: ").append(auditEvent.planCpuCosts).append("\n")
                        .append(prefix).append("Memory: ").append(auditEvent.planMemCosts).append("\n\n");
            }
        }

        if (level == null) {
            str.append(Explain.toString(physicalPlan, outputColumns));
        } else {
            if (planCount != 0) {
                str.append("There are ").append(planCount).append(" plans in optimizer search space\n");
            }
            if (useBaseline > 0) {
                str.append("Using baseline plan[").append(useBaseline).append("]\n\n");
            }

            for (int i = 0; i < fragments.size(); ++i) {
                PlanFragment fragment = fragments.get(i);
                if (i > 0) {
                    // a blank line between plan fragments
                    str.append("\n");
                }
                if (level.equals(TExplainLevel.NORMAL)) {
                    str.append("PLAN FRAGMENT ").append(i).append("\n");
                    str.append(fragment.getExplainString(TExplainLevel.NORMAL));
                } else if (level.equals(TExplainLevel.COSTS)) {
                    str.append("PLAN FRAGMENT ").append(i).append("(").append(fragment.getFragmentId()).append(")\n");
                    str.append(fragment.getCostExplain());
                } else {
                    str.append("PLAN FRAGMENT ").append(i).append("(").append(fragment.getFragmentId()).append(")\n");
                    str.append(fragment.getVerboseExplain());
                }
            }
        }
        return str.toString();
    }

    public String getExplainString(StatementBase.ExplainLevel level) {
        TExplainLevel tlevel = null;
        switch (level) {
            case NORMAL:
                tlevel = TExplainLevel.NORMAL;
                break;
            case VERBOSE:
                tlevel = TExplainLevel.VERBOSE;
                break;
            case COSTS:
                tlevel = TExplainLevel.COSTS;
                break;
        }
        return getExplainString(tlevel);
    }

    public LogicalPlan getLogicalPlan() {
        return logicalPlan;
    }

    public void setLogicalPlan(LogicalPlan logicalPlan) {
        this.logicalPlan = logicalPlan;
    }

    public ColumnRefFactory getColumnRefFactory() {
        return columnRefFactory;
    }

    public void setColumnRefFactory(ColumnRefFactory columnRefFactory) {
        this.columnRefFactory = columnRefFactory;
    }

    public List<Integer> getCollectExecStatsIds() {
        return collectExecStatsIds;
    }

    public void setCollectExecStatsIds(List<Integer> collectExecStatsIds) {
        this.collectExecStatsIds = collectExecStatsIds;
    }

    public boolean isShortCircuit() {
        return isShortCircuit;
    }
}