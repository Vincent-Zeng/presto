package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.FunctionHandle;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.analyzer.SymbolAllocator;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SinkNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.SINGLE;

/**
 * Splits a logical plan into fragments that can be shipped and executed on distributed nodes
 */
public class DistributedLogicalPlanner
{
    private final Metadata metadata;

    public DistributedLogicalPlanner(Metadata metadata)
    {
        this.metadata = metadata;
    }

    public SubPlan createSubplans(PlanNode plan, SymbolAllocator allocator, boolean createSingleNodePlan)
    {
        // zeng: TODO
        Visitor visitor = new Visitor(allocator, createSingleNodePlan);
        SubPlanBuilder builder = plan.accept(visitor, null);

        // zeng: TODO
        SubPlan subplan = builder.build();

        // zeng: TODO
        subplan.sanityCheck();

        return subplan;
    }

    private class Visitor
            extends PlanVisitor<Void, SubPlanBuilder>
    {
        private int fragmentId = 0;

        private final SymbolAllocator allocator;
        private final boolean createSingleNodePlan;

        public Visitor(SymbolAllocator allocator, boolean createSingleNodePlan)
        {
            this.allocator = allocator;
            this.createSingleNodePlan = createSingleNodePlan;
        }

        @Override
        public SubPlanBuilder visitAggregation(AggregationNode node, Void context)
        {
            // zeng: visit 依赖节点
            SubPlanBuilder current = node.getSource().accept(this, context);

            if (!current.isPartitioned()) { // zeng: 不是多个partition
                // add the aggregation node as the root of the current fragment
                // zeng: 同一个计划片段 aggregation node 加入dag
                current.setRoot(new AggregationNode(current.getRoot(), node.getGroupBy(), node.getAggregations(), node.getFunctions(), SINGLE));
                return current;
            }

            // else, we need to "close" the current fragment and create an unpartitioned fragment for the final aggregation

            Map<Symbol, FunctionCall> finalCalls = new HashMap<>();
            Map<Symbol, FunctionCall> intermediateCalls = new HashMap<>();
            Map<Symbol, FunctionHandle> intermediateFunctions = new HashMap<>();

            // zeng: 每个partition上聚合的function call
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                FunctionHandle functionHandle = node.getFunctions().get(entry.getKey());
                FunctionInfo function = metadata.getFunction(functionHandle);

                Symbol intermediateSymbol = allocator.newSymbol(function.getName().getSuffix(), Type.fromRaw(function.getIntermediateType()));
                intermediateCalls.put(intermediateSymbol, entry.getValue());
                intermediateFunctions.put(intermediateSymbol, functionHandle);

                // rewrite final aggregation in terms of intermediate function
                finalCalls.put(entry.getKey(), new FunctionCall(function.getName(), ImmutableList.<Expression>of(new QualifiedNameReference(intermediateSymbol.toQualifiedName()))));
            }

            // zeng: 部分聚合 aggregation node
            AggregationNode aggregation = new AggregationNode(current.getRoot(), node.getGroupBy(), intermediateCalls, intermediateFunctions, PARTIAL);
            // zeng: 当前计划片段 加入 sink node
            current.setRoot(new SinkNode(current.getId(), aggregation));

            // create merge + aggregation plan

            // zeng: exchange node
            ExchangeNode source = new ExchangeNode(
                    current.getId(),    // zeng: 依赖的计划片段id
                    current.getRoot().getOutputSymbols()    // zeng: 期望输出列
            );
            // zeng: 所有partition的聚合结果进行最终聚合 aggregation node
            AggregationNode merged = new AggregationNode(
                    source, // zeng: exchange node
                    node.getGroupBy(),
                    finalCalls,
                    node.getFunctions(),
                    FINAL
            );

            // zeng: 新的 计划片段
            return newSubPlan(merged)
                    .setPartitioned(false)  // zeng: 不是多分区 TODO 为什么
                    .addChild(current.build()); // zeng: 依赖的计划片段
        }

        @Override
        public SubPlanBuilder visitFilter(FilterNode node, Void context)
        {
            // zeng: visit 依赖节点
            SubPlanBuilder current = node.getSource().accept(this, context);

            // zeng: 同一个计划片段
            current.setRoot(
                    new FilterNode( // zeng: 同一个计划片段里的dag
                            current.getRoot(),  // zeng: 依赖的 plan node
                            node.getPredicate()
                    )
            );
            return current;
        }

        @Override
        public SubPlanBuilder visitProject(ProjectNode node, Void context)
        {
            // zeng: visit依赖节点
            SubPlanBuilder current = node.getSource().accept(this, context);

            // zeng: 当前计划片段  project node 加入 dag
            current.setRoot(new ProjectNode(current.getRoot(), node.getOutputMap()));

            return current;
        }

        @Override
        public SubPlanBuilder visitTopN(TopNNode node, Void context)
        {
            // zeng: visit 依赖节点
            SubPlanBuilder current = node.getSource().accept(this, context);

            // zeng: 当前计划片段 topN node 加入 dag
            current.setRoot(new TopNNode(current.getRoot(), node.getCount(), node.getOrderBy(), node.getOrderings()));

            if (current.isPartitioned()) {  // zeng: 多个分区
                // zeng: 当前计划片段加入sink node
                current.setRoot(new SinkNode(current.getId(), current.getRoot()));

                // create merge plan fragment

                // zeng: exchange node
                PlanNode source = new ExchangeNode(
                        current.getId(),    // zeng: 依赖片段id
                        current.getRoot().getOutputSymbols()
                );
                // zeng: topN node
                TopNNode merge = new TopNNode(
                        source, // zeng: 依赖exchange node
                        node.getCount(),
                        node.getOrderBy(),
                        node.getOrderings()
                );

                // zeng: 新的计划片段
                current = newSubPlan(merge) // zeng: topN node
                        .setPartitioned(false)  // zeng: 没有多个分区
                        .addChild(current.build()); // zeng: 依赖的计划片段
            }

            return current;
        }

        @Override
        public SubPlanBuilder visitSort(SortNode node, Void context)
        {
            // zeng: visit 依赖节点
            SubPlanBuilder current = node.getSource().accept(this, context);

            if (current.isPartitioned()) {
                // zeng: 当前计划片段 sink node 加入 dag
                current.setRoot(new SinkNode(current.getId(), current.getRoot()));

                // create a new non-partitioned fragment
                // zeng: 新的计划片段
                current = newSubPlan(
                        // zeng: 依赖 exchange node
                        new ExchangeNode(
                                current.getId(),    // zeng: 依赖的计划片段id
                                current.getRoot().getOutputSymbols()
                        )
                )
                        .setPartitioned(false)  // zeng: 没有多partition
                        .addChild(current.build()); // zeng: 依赖的计划片段
            }

            // zeng: sort node 加入 dag
            current.setRoot(new SortNode(current.getRoot(), node.getOrderBy(), node.getOrderings()));

            return current;
        }


        @Override
        public SubPlanBuilder visitOutput(OutputNode node, Void context)
        {
            // zeng: 访问依赖节点
            SubPlanBuilder current = node.getSource().accept(this, context);

            if (current.isPartitioned()) {
                // zeng: 当前依赖片段 sink node 加入 dag
                current.setRoot(new SinkNode(current.getId(), current.getRoot()));

                // create a new non-partitioned fragment

                current = newSubPlan(   // zeng: 新的计划片段
                        new ExchangeNode(   // zeng: exchange node
                                current.getId(),    // zeng: 依赖片段id
                                current.getRoot().getOutputSymbols()
                        )
                )
                        .setPartitioned(false)  // zeng: 没有多分区
                        .addChild(current.build()); // zeng: 依赖的计划片段
            }

            // zeng: output node 加入 dag
            current.setRoot(new OutputNode(current.getRoot(), node.getColumnNames(), node.getAssignments()));

            return current;
        }

        @Override
        public SubPlanBuilder visitLimit(LimitNode node, Void context)
        {
            // zeng: visit 依赖的节点
            SubPlanBuilder current = node.getSource().accept(this, context);

            // zeng: 当前计划片段 limit node 加入dag
            current.setRoot(new LimitNode(current.getRoot(), node.getCount()));

            if (current.isPartitioned()) {
                // zeng: 当前计划片段 sink node 加入dag
                current.setRoot(new SinkNode(current.getId(), current.getRoot()));

                // create merge plan fragment
                // zeng: exchange node
                PlanNode source = new ExchangeNode(
                        current.getId(),    // zeng: 依赖的计划片段id
                        current.getRoot().getOutputSymbols()
                );
                // zeng: limit node
                LimitNode merge = new LimitNode(
                        source, // zeng: exchange node
                        node.getCount()
                );

                // zeng: 新的计划片段
                current = newSubPlan(merge) // zeng: limit node
                        .setPartitioned(false) // zeng: 没有多分区
                        .addChild(current.build()); // zeng: 依赖的计划片段
            }

            return current;
        }

        @Override
        public SubPlanBuilder visitTableScan(TableScanNode node, Void context)
        {
            return newSubPlan(node) // zeng: 新的计划片段
                    .setPartitioned(!createSingleNodePlan); // zeng: 是否多个partition
        }

        @Override
        public SubPlanBuilder visitJoin(JoinNode node, Void context)
        {
            // zeng: left 一个计划片段
            SubPlanBuilder left = node.getLeft().accept(this, context);
            // zeng: right 一个计划片段
            SubPlanBuilder right = node.getRight().accept(this, context);

            if (left.isPartitioned() || right.isPartitioned()) {
                // zeng: TODO

                right.setRoot(new SinkNode(right.getId(), right.getRoot()));
                ExchangeNode exchange = new ExchangeNode(right.getId(), right.getRoot().getOutputSymbols());

                JoinNode join = new JoinNode(left.getRoot(), exchange, node.getCriteria());

                left.setRoot(join)
                    .addChild(right.build());

                return left;
            }
            else {
                // zeng: join node
                JoinNode join = new JoinNode(left.getRoot(), right.getRoot(), node.getCriteria());

                // zeng: 新的计划片段
                return newSubPlan(join) // zeng: join node
                        .setPartitioned(false)  // zeng: 没有多个partition
                        .setChildren(Iterables.concat(left.getChildren(), right.getChildren()));    // zeng: 依赖的计划片段
            }
        }

        @Override
        protected SubPlanBuilder visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        private SubPlanBuilder newSubPlan(PlanNode root)
        {
            // zeng: 片段id， symbol分配器, plan node
            return new SubPlanBuilder(fragmentId++, allocator, root);
        }
    }

}
