package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.FunctionHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.sql.analyzer.AnalysisResult;
import com.facebook.presto.sql.analyzer.AnalyzedFunction;
import com.facebook.presto.sql.analyzer.AnalyzedExpression;
import com.facebook.presto.sql.analyzer.AnalyzedJoinClause;
import com.facebook.presto.sql.analyzer.AnalyzedOrdering;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.analyzer.SymbolAllocator;
import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.optimizations.CoalesceLimits;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PruneRedundantProjections;
import com.facebook.presto.sql.planner.optimizations.PruneUnreferencedOutputs;
import com.facebook.presto.sql.planner.optimizations.SimplifyExpressions;
import com.facebook.presto.sql.planner.optimizations.UnaliasSymbolReferences;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRewriter;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Subquery;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.analyzer.AnalyzedFunction.argumentGetter;
import static com.facebook.presto.sql.analyzer.AnalyzedOrdering.expressionGetter;
import static com.google.common.collect.Iterables.concat;

public class LogicalPlanner
{
    private final Metadata metadata;
    private final Session session;

    public LogicalPlanner(Session session, Metadata metadata)
    {
        this.metadata = metadata;
        this.session = session;
    }

    public PlanNode plan(AnalysisResult analysis)
    {
        // zeng: 构建计划树(plan node dag)
        PlanNode root = createOutputPlan(analysis);

        // zeng: 校验计划树
        // make sure we produce a valid plan. This is mainly to catch programming errors
        PlanSanityChecker.validate(root);

        // zeng: symbol -> Type 映射
        Map<Symbol, Type> types = analysis.getTypes();

        // zeng: 优化计划树
        for (PlanOptimizer optimizer : getOptimizations()) {
            root = optimizer.optimize(root, types);
        }

        return root;
    }

    private List<PlanOptimizer> getOptimizations()
    {
        return ImmutableList.of(
                new SimplifyExpressions(metadata, session),
                new PruneUnreferencedOutputs(),
                new UnaliasSymbolReferences(),
                new PruneRedundantProjections(),
                new CoalesceLimits()
        );
    }

    private PlanNode createOutputPlan(AnalysisResult analysis)
    {
        // zeng: query plan node
        PlanNode result = createQueryPlan(analysis);

        int i = 0;
        List<String> names = new ArrayList<>();
        ImmutableMap.Builder<String, Symbol> assignments = ImmutableMap.builder();
        for (Field field : analysis.getOutputDescriptor().getFields()) {
            // zeng: column name
            String name = field.getAttribute().orNull();

            // zeng: 使列名唯一
            while (name == null || names.contains(name)) {
                // TODO: this shouldn't be necessary once OutputNode uses Multimaps (requires updating to Jackson 2 for serialization support)
                i++;
                name = "_col" + i;
            }
            names.add(name);

            // zeng: 列名 -> symbol map
            assignments.put(name, field.getSymbol());
        }

        // zeng: output plan node
        // zeng: query plan node, column name list， column name -> symbol map
        return new OutputNode(result, names, assignments.build());
    }

    private PlanNode createQueryPlan(AnalysisResult analysis)
    {
        // zeng: Query子树
        Query query = analysis.getRewrittenQuery();

        // zeng: 叶子 from plan node
        PlanNode root = createRelationPlan(query.getFrom(), analysis);

        if (analysis.getPredicate() != null) {
            // zeng: filter plan node
            // zeng: 参数为 from plan node， where clause expression
            root = createFilterPlan(root, analysis.getPredicate());
        }

        // zeng: rewritten expression -> symbol map
        Map<Expression, Symbol> substitutions = new HashMap<>();

        if (!analysis.getAggregations().isEmpty() || !analysis.getGroupByExpressions().isEmpty()) {
            // zeng: project node <- aggregate node <- project node
            root = createAggregatePlan(
                    root,
                    ImmutableList.copyOf(analysis.getOutputExpressions().values()), // zeng: select clause symbol -> analysed expression map
                    Lists.transform(analysis.getOrderBy(), expressionGetter()), // zeng: order by clause analysed expression getter
                    analysis.getAggregations(), // zeng: aggregate analysed function list
                    analysis.getGroupByExpressions(), // zeng: group by clause analysed expression list
                    analysis.getSymbolAllocator(),  // zeng: symbol 分配器
                    substitutions
            );
        }

        if (analysis.isDistinct()) {
            // zeng: TODO 有必要吗  aggregate这一步不是已经映射过了？
            root = createProjectPlan(root, analysis.getOutputExpressions(), substitutions); // project query outputs
            // zeng: aggregate node
            root = createDistinctPlan(root);
        }

        if (!analysis.getOrderBy().isEmpty()) {
            if (analysis.getLimit() != null) {
                // zeng: top n node
                root = createTopNPlan(
                        root,
                        analysis.getLimit(),    // zeng: limit
                        analysis.getOrderBy(),  // zeng: AnalysedOrdering list
                        analysis.getSymbolAllocator(),  // zeng: symbol分配器
                        substitutions   // zeng: rewritten expression -> symbol map
                );
            }
            else {
                // zeng: sort node
                root = createSortPlan(
                        root,
                        analysis.getOrderBy(),  // zeng: AnalysedOrdering list
                        analysis.getSymbolAllocator(),  // zeng: symbol分配器
                        substitutions   // zeng: rewritten expression -> symbol map
                );
            }
        }

        if (!analysis.isDistinct()) {
            // zeng: TODO 为啥
            root = createProjectPlan(root, analysis.getOutputExpressions(), substitutions); // project query outputs
        }

        if (analysis.getLimit() != null && analysis.getOrderBy().isEmpty()) {
            // zeng: limit node
            root = createLimitPlan(root, analysis.getLimit());
        }

        return root;
    }

    private PlanNode createDistinctPlan(PlanNode source)
    {
        // zeng: 等同于 group by 所有 output 列
        // zeng： aggregate node
        AggregationNode aggregation = new AggregationNode(source, source.getOutputSymbols(), ImmutableMap.<Symbol, FunctionCall>of(), ImmutableMap.<Symbol, FunctionHandle>of());
        return aggregation;
    }

    private PlanNode createLimitPlan(PlanNode source, long limit)
    {
        // zeng: limit node
        return new LimitNode(source, limit);
    }

    private PlanNode createTopNPlan(PlanNode source, long limit, List<AnalyzedOrdering> orderBy, SymbolAllocator allocator, Map<Expression, Symbol> substitutions)
    {
        /**
         * Turns SELECT $10, $11 ORDER BY expr($0, $1,...), expr($2, $3, ...) LIMIT c into
         *
         * - TopN [c] order by $4, $5
         *     - Project $4 = expr($0, $1, ...), $5 = expr($2, $3, ...), $10, $11
         */

        Map<Symbol, Expression> preProjectAssignments = new HashMap<>();
        for (Symbol symbol : source.getOutputSymbols()) {
            // propagate all output symbols from underlying operator
            QualifiedNameReference expression = new QualifiedNameReference(symbol.toQualifiedName());
            preProjectAssignments.put(symbol, expression);
        }

        List<Symbol> orderBySymbols = new ArrayList<>();
        Map<Symbol,SortItem.Ordering> orderings = new HashMap<>();
        for (AnalyzedOrdering item : orderBy) {
            Expression rewritten = TreeRewriter.rewriteWith(substitution(substitutions), item.getExpression().getRewrittenExpression());

            Symbol symbol = allocator.newSymbol(rewritten, item.getExpression().getType());

            // zeng: order by clause symbol list
            orderBySymbols.add(symbol);
            // zeng: symbol -> expression map
            preProjectAssignments.put(symbol, rewritten);
            // zeng: symbol -> Ordering map
            orderings.put(symbol, item.getOrdering());
        }

        // zeng: pre project node
        ProjectNode preProject = new ProjectNode(source, preProjectAssignments);

        // zeng: topN node
        // zeng: 参数为 pre project node, limit, order by clause symbol list, symbol -> Ordering map
        return new TopNNode(preProject, limit, orderBySymbols, orderings);
    }

    private PlanNode createSortPlan(PlanNode source, List<AnalyzedOrdering> orderBy, SymbolAllocator allocator, Map<Expression, Symbol> substitutions)
    {
        Map<Symbol, Expression> preProjectAssignments = new HashMap<>();
        for (Symbol symbol : source.getOutputSymbols()) {
            // propagate all output symbols from underlying operator
            QualifiedNameReference expression = new QualifiedNameReference(symbol.toQualifiedName());
            preProjectAssignments.put(symbol, expression);
        }

        List<Symbol> orderBySymbols = new ArrayList<>();
        Map<Symbol,SortItem.Ordering> orderings = new HashMap<>();
        for (AnalyzedOrdering item : orderBy) {
            Expression rewritten = TreeRewriter.rewriteWith(substitution(substitutions), item.getExpression().getRewrittenExpression());

            Symbol symbol = allocator.newSymbol(rewritten, item.getExpression().getType());

            // zeng: order by clause symbol list
            orderBySymbols.add(symbol);
            // zeng: symbol -> expression map
            preProjectAssignments.put(symbol, rewritten);
            // zeng: symbol -> Ordering map
            orderings.put(symbol, item.getOrdering());
        }

        // zeng: project node
        ProjectNode preProject = new ProjectNode(source, preProjectAssignments);

        // zeng: sort node
        // zeng: 参数为 pre project node, order by clause symbol list, symbol -> Ordering map
        return new SortNode(preProject, orderBySymbols, orderings);
    }

    private PlanNode createAggregatePlan(
            PlanNode source,
            List<AnalyzedExpression> outputs,
            List<AnalyzedExpression> orderBy,
            Set<AnalyzedFunction> aggregations,
            List<AnalyzedExpression> groupBys,
            SymbolAllocator allocator,
            Map<Expression, Symbol> outputSubstitutions
    )
    {
        /**
         * Turns SELECT k1 + 1, sum(v1 * v2) - sum(v3 * v4) GROUP BY k1 + 1, k2 into
         *
         * 3. Project $0, $1, $7 = $5 - $6
         *   2. Aggregate by ($0, $1): $5 = sum($3), $6 = sum($4)
         *     1. Project $0 = k1 + 1, $1 = k2, $3 = v1 * v2, $4 = v3 * v4
         */

        // 1. Pre-project all scalar inputs
        Set<AnalyzedExpression> scalarExpressions = ImmutableSet.copyOf(
                concat(
                    IterableTransformer.on(aggregations)    // zeng: parameter of all aggregate function
                        .transformAndFlatten(argumentGetter())
                        .list(),
                    groupBys    // zeng: group by clause expression list
                )
        );

        BiMap<Symbol, Expression> scalarAssignments = HashBiMap.create();
        for (AnalyzedExpression expression : scalarExpressions) {
            // zeng: scalar expression symbol
            Symbol symbol = allocator.newSymbol(expression.getRewrittenExpression(), expression.getType());
            // zeng: symbol -> scalar expression map
            scalarAssignments.put(symbol, expression.getRewrittenExpression());
        }

        PlanNode preProjectNode = source;
        if (!scalarAssignments.isEmpty()) { // workaround to deal with COUNT's lack of inputs
            // zeng: pre project plan node
            preProjectNode = new ProjectNode(source, scalarAssignments);
        }

        // 2. Aggregate
        Map<Expression, Symbol> substitutions = new HashMap<>();

        BiMap<Symbol, FunctionCall> aggregationAssignments = HashBiMap.create();
        Map<Symbol, FunctionHandle> functions = new HashMap<>();
        for (AnalyzedFunction aggregation : aggregations) {
            // rewrite function calls in terms of scalar inputs
            // zeng: 改写方法参数expression为symbol
            FunctionCall rewrittenFunction = TreeRewriter.rewriteWith(substitution(scalarAssignments.inverse()), aggregation.getRewrittenCall());
            // zeng: function name symbol
            Symbol symbol = allocator.newSymbol(aggregation.getFunctionName().getSuffix(), aggregation.getType());
            // zeng: symbol -> function call map
            aggregationAssignments.put(symbol, rewrittenFunction);

            // zeng: symbol -> function handler
            functions.put(symbol, aggregation.getFunctionInfo().getHandle());

            // zeng: rewritten function call -> symbol map
            // build substitution map to rewrite assignments in post-project
            substitutions.put(aggregation.getRewrittenCall(), symbol);
        }

        List<Symbol> groupBySymbols = new ArrayList<>();
        for (AnalyzedExpression groupBy : groupBys) {
            // zeng:  group by clause expression symbol
            Symbol symbol = scalarAssignments.inverse().get(groupBy.getRewrittenExpression());

            // zeng: rewritten expression -> symbol map
            substitutions.put(groupBy.getRewrittenExpression(), symbol);
            // zeng: group by clause  symbol list
            groupBySymbols.add(symbol);
        }

        // zeng: aggregate plan node
        // zeng: 参数为 pre project plan node,  group by symbol list,  symbol -> function call map,  symbol -> function handler map
        PlanNode aggregationNode = new AggregationNode(preProjectNode, groupBySymbols, aggregationAssignments, functions);

        // 3. Post-project scalar expressions based on aggregations
        BiMap<Symbol, Expression> postProjectScalarAssignments = HashBiMap.create();
        for (AnalyzedExpression expression : ImmutableSet.copyOf(concat(
                outputs,    // zeng: select clause analysed expression list
                groupBys,   // zeng: group by clause analysed expression list
                orderBy     // eng: order by clause analysed expression list
        ))) {
            // zeng: expression中改写上面已改写的部分
            Expression rewritten = TreeRewriter.rewriteWith(substitution(substitutions), expression.getRewrittenExpression());
            // zeng: 分配symbol
            Symbol symbol = allocator.newSymbol(rewritten, expression.getType());

            // zeng: symbol -> expression map
            postProjectScalarAssignments.put(symbol, rewritten);

            // zeng: rewritten expression -> symbol map
            // build substitution map to return to caller
            outputSubstitutions.put(expression.getRewrittenExpression(), symbol);
        }

        // zeng: post project node
        // zeng: 参数为 aggregate node, symbol -> expression map
        return new ProjectNode(aggregationNode, postProjectScalarAssignments);
    }

    private PlanNode createProjectPlan(PlanNode root, Map<Symbol, AnalyzedExpression> outputAnalysis, final Map<Expression, Symbol> substitutions)
    {
        Map<Symbol, Expression> outputs = Maps.transformValues(outputAnalysis, new Function<AnalyzedExpression, Expression>()
        {
            @Override
            public Expression apply(AnalyzedExpression input)
            {
                return TreeRewriter.rewriteWith(substitution(substitutions), input.getRewrittenExpression());
            }
        });

        return new ProjectNode(root, outputs);
    }

    private FilterNode createFilterPlan(PlanNode source, AnalyzedExpression predicate)
    {
        // zeng: filter plan node
        return new FilterNode(source, predicate.getRewrittenExpression());
    }

    private PlanNode createRelationPlan(List<Relation> relations, AnalysisResult analysis)
    {
        // zeng: 当前不支持多表
        Relation relation = Iterables.getOnlyElement(relations); // TODO: add join support

        if (relation instanceof Table) {
            // zeng: scan node
            return createScanNode((Table) relation, analysis);
        }
        else if (relation instanceof AliasedRelation) {
            // zeng: TODO
            return createRelationPlan(ImmutableList.of(((AliasedRelation) relation).getRelation()), analysis);
        }
        else if (relation instanceof Subquery) {
            // zeng: TODO
            AnalysisResult subqueryAnalysis = analysis.getAnalysis((Subquery) relation);
            return createQueryPlan(subqueryAnalysis);
        }
        else if (relation instanceof Join) {
            // zeng: TODO
            return createJoinPlan((Join) relation, analysis);
        }

        throw new UnsupportedOperationException("not yet implemented");
    }

    private PlanNode createJoinPlan(Join join, AnalysisResult analysis)
    {
        PlanNode leftPlan = createRelationPlan(ImmutableList.of(join.getLeft()), analysis);
        PlanNode rightPlan = createRelationPlan(ImmutableList.of(join.getRight()), analysis);

        // We insert a projection on the left side and right side blindly -- they'll get optimized out later if not needed
        Map<Symbol, Expression> leftProjections = new HashMap<>();
        Map<Symbol, Expression> rightProjections = new HashMap<>();

        // add a pass-through projection for the outputs of left and right
        for (Symbol symbol : leftPlan.getOutputSymbols()) {
            leftProjections.put(symbol, new QualifiedNameReference(symbol.toQualifiedName()));
        }
        for (Symbol symbol : rightPlan.getOutputSymbols()) {
            rightProjections.put(symbol, new QualifiedNameReference(symbol.toQualifiedName()));
        }

        // next, handle the join clause...
        List<AnalyzedJoinClause> criteria = analysis.getJoinCriteria(join);

        ImmutableList.Builder<JoinNode.EquiJoinClause> equiJoinClauses = ImmutableList.builder();
        for (AnalyzedJoinClause analyzedClause : criteria) {
            // insert a projection for the sub-expression corresponding to the left side and assign it to
            // a new symbol. If the expression is already a simple symbol reference, this will result in an identity projection
            AnalyzedExpression left = analyzedClause.getLeft();
            Symbol leftSymbol = analysis.getSymbolAllocator().newSymbol(left.getRewrittenExpression(), left.getType());
            leftProjections.put(leftSymbol, left.getRewrittenExpression());

            // do the same for the right side
            AnalyzedExpression right = analyzedClause.getRight();
            Symbol rightSymbol = analysis.getSymbolAllocator().newSymbol(right.getRewrittenExpression(), right.getType());
            rightProjections.put(rightSymbol, right.getRewrittenExpression());

            equiJoinClauses.add(new JoinNode.EquiJoinClause(leftSymbol, rightSymbol));
        }

        leftPlan = new ProjectNode(leftPlan, leftProjections);
        rightPlan = new ProjectNode(rightPlan, rightProjections);

        return new JoinNode(leftPlan, rightPlan, equiJoinClauses.build());
    }

    private PlanNode createScanNode(Table table, AnalysisResult analysis)
    {
        // zeng: 列信息
        TupleDescriptor descriptor = analysis.getTableDescriptor(table);
        // zeng: 表元数据
        TableMetadata metadata = analysis.getTableMetadata(table);

        ImmutableMap.Builder<Symbol, ColumnHandle> columns = ImmutableMap.builder();

        // zeng: 表所有字段
        List<Field> fields = descriptor.getFields();

        // zeng: symbol -> column handler map
        for (Field field : fields) {
            columns.put(field.getSymbol(), field.getColumn().get());
        }

        // zeng: TODO 没有存表名?
        // zeng: table datasource type, symbol -> column map
        return new TableScanNode(metadata.getTableHandle().get(), columns.build());
    }

    private NodeRewriter<Void> substitution(final Map<Expression, Symbol> substitutions)
    {
        return new NodeRewriter<Void>()
        {
            @Override
            public Node rewriteExpression(Expression node, Void context, TreeRewriter<Void> treeRewriter)
            {
                Symbol symbol = substitutions.get(node);
                if (symbol != null) {
                    return new QualifiedNameReference(symbol.toQualifiedName());
                }

                return treeRewriter.defaultRewrite(node, context);
            }
        };
    }
}
