package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.analyzer.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Predicates.in;

public class SubPlanBuilder
{
    private final int id;
    private PlanNode root;
    private boolean isPartitioned;
    private List<SubPlan> children = new ArrayList<>();

    private final SymbolAllocator allocator;

    public SubPlanBuilder(int id, SymbolAllocator allocator, PlanNode root)
    {
        Preconditions.checkArgument(id >= 0, "id must be >= 0");
        Preconditions.checkNotNull(allocator, "allocator is null");
        Preconditions.checkNotNull(root, "root is null");

        this.allocator = allocator;
        this.id = id;
        this.root = root;
    }

    public int getId()
    {
        return id;
    }

    public SubPlanBuilder setRoot(PlanNode root)
    {
        Preconditions.checkNotNull(root, "root is null");
        this.root = root;
        return this;
    }

    public SubPlanBuilder setPartitioned(boolean partitioned)
    {
        isPartitioned = partitioned;
        return this;
    }

    public PlanNode getRoot()
    {
        return root;
    }

    public boolean isPartitioned()
    {
        return isPartitioned;
    }

    public SubPlanBuilder setChildren(Iterable<SubPlan> children)
    {
        this.children = Lists.newArrayList(children);
        return this;
    }

    public SubPlanBuilder addChild(SubPlan child)
    {
        this.children.add(child);
        return this;
    }

    public List<SubPlan> getChildren()
    {
        return children;
    }

    public SubPlan build()
    {
        // zeng: 依赖的symbol列表
        Set<Symbol> dependencies = SymbolExtractor.extract(root);

        // zeng: 计划片段
        PlanFragment fragment = new PlanFragment(
                id,
                isPartitioned,  // zeng: 是否分成多个partition
                Maps.filterKeys(allocator.getTypes(), in(dependencies)),    // zeng: 依赖的symbol -> Type 映射
                root    // zeng: plan node
        );

        // zeng: 分布式逻辑计划子树
        // zeng: 参数为 本计划片段， 依赖的计划子树
        return new SubPlan(fragment, children);
    }
}
