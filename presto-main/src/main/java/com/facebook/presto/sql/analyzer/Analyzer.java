package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.LookupSymbolResolver;
import com.facebook.presto.sql.planner.SymbolResolver;
import com.facebook.presto.sql.tree.AliasedExpression;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.JoinUsing;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowPartitions;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.Subquery;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.metadata.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static com.facebook.presto.metadata.InformationSchemaMetadata.TABLE_COLUMNS;
import static com.facebook.presto.metadata.InformationSchemaMetadata.TABLE_INTERNAL_FUNCTIONS;
import static com.facebook.presto.metadata.InformationSchemaMetadata.TABLE_INTERNAL_PARTITIONS;
import static com.facebook.presto.metadata.InformationSchemaMetadata.TABLE_TABLES;
import static com.facebook.presto.sql.analyzer.AnalyzedExpression.rewrittenExpressionGetter;
import static com.facebook.presto.sql.analyzer.AnalyzedFunction.distinctPredicate;
import static com.facebook.presto.sql.analyzer.AnalyzedOrdering.expressionGetter;
import static com.facebook.presto.sql.analyzer.AnalyzedOrdering.nodeGetter;
import static com.facebook.presto.sql.analyzer.Field.nameGetter;
import static com.facebook.presto.sql.tree.QueryUtil.aliasedName;
import static com.facebook.presto.sql.tree.QueryUtil.ascending;
import static com.facebook.presto.sql.tree.QueryUtil.caseWhen;
import static com.facebook.presto.sql.tree.QueryUtil.equal;
import static com.facebook.presto.sql.tree.QueryUtil.functionCall;
import static com.facebook.presto.sql.tree.QueryUtil.logicalAnd;
import static com.facebook.presto.sql.tree.QueryUtil.nameReference;
import static com.facebook.presto.sql.tree.QueryUtil.selectAll;
import static com.facebook.presto.sql.tree.QueryUtil.selectList;
import static com.facebook.presto.sql.tree.QueryUtil.table;
import static com.facebook.presto.sql.tree.SortItem.sortKeyGetter;
import static com.google.common.base.Predicates.compose;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;

public class Analyzer
{
    private final Metadata metadata;
    private final Session session;

    public Analyzer(Session session, Metadata metadata)
    {
        this.session = session;
        this.metadata = metadata;
    }

    public AnalysisResult analyze(Node node)
    {
        return analyze(
                node,   // zeng: node
                new AnalysisContext(session)    // zeng: AnalysisContext
        );
    }

    private AnalysisResult analyze(Node node, AnalysisContext context)
    {
        // zeng: StatementAnalyzer.process
        StatementAnalyzer analyzer = new StatementAnalyzer(metadata);
        return analyzer.process(node, context);
    }

    private static class StatementAnalyzer
            extends AstVisitor<AnalysisResult, AnalysisContext>
    {
        private final Metadata metadata;

        private StatementAnalyzer(Metadata metadata)
        {
            this.metadata = metadata;
        }

        @Override
        protected AnalysisResult visitStatement(Statement node, AnalysisContext context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        // zeng: query 逻辑计划
        @Override
        protected AnalysisResult visitQuery(Query query, AnalysisContext context)
        {
            // zeng: 不支持having?
            Preconditions.checkArgument(query.getHaving() == null, "not yet implemented: HAVING");
            // zeng: 不支持join？
            Preconditions.checkArgument(query.getFrom().size() == 1, "not yet implemented: multiple FROM relations");

            // prevent symbol allocator from picking symbols named the same as output aliases, since both share the same namespace for reference resolution
            for (Expression expression : query.getSelect().getSelectItems()) {  // zeng: select后的表达式， 包括 * 和 T.* 这种
                if (expression instanceof AliasedExpression) {   // zeng: 带列别名
                    // zeng: 别名 不能作为 column symbol
                    context.getSymbolAllocator().blacklist(((AliasedExpression) expression).getAlias());
                }
                else if (expression instanceof QualifiedNameReference) {    // zeng: 带限定名
                    // zeng: 限定名 不能作为 column symbol
                    context.getSymbolAllocator().blacklist(((QualifiedNameReference) expression).getName().toString());
                }
            }

            // analyze FROM clause
            // zeng: Relation 有 Table Join Subquery AliasedRelation 等子类
            Relation relation = Iterables.getOnlyElement(query.getFrom());
            // zeng: RelationAnalyzer.visitXxx() 返回 列信息
            TupleDescriptor sourceDescriptor = new RelationAnalyzer(metadata, context.getSession())
                    .process(relation, context);

            AnalyzedExpression predicate = null;
            if (query.getWhere() != null) {
                // zeng:  解析 where 后的expression
                predicate = analyzePredicate(query.getWhere(), sourceDescriptor);
            }

            // zeng: 解析group by后的expression
            List<AnalyzedExpression> groupBy = analyzeGroupBy(query.getGroupBy(), sourceDescriptor);
            // zeng: 解析select 和 order by 得到所有的AnalyzedFunction
            Set<AnalyzedFunction> aggregations = analyzeAggregations(query.getGroupBy(), query.getSelect(), query.getOrderBy(), sourceDescriptor);
            // zeng: TODO
            List<AnalyzedOrdering> orderBy = analyzeOrderBy(query.getOrderBy(), sourceDescriptor);
            // zeng: TODO
            AnalyzedOutput output = analyzeOutput(query.getSelect(), context.getSymbolAllocator(), sourceDescriptor);

            if (query.getSelect().isDistinct()) {
                // zeng: TODO
                analyzeDistinct(output, orderBy);
            }

            Long limit = null;
            if (query.getLimit() != null) {
                // zeng: limit
                limit = Long.parseLong(query.getLimit());
            }

            // zeng: TODO
            return AnalysisResult.newInstance(context, query.getSelect().isDistinct(), output, predicate, groupBy, aggregations, limit, orderBy, query);
        }

        @Override
        protected AnalysisResult visitShowTables(ShowTables showTables, AnalysisContext context)
        {
            String catalogName = context.getSession().getCatalog();
            String schemaName = context.getSession().getSchema();

            QualifiedName schema = showTables.getSchema();
            if (schema != null) {
                List<String> parts = schema.getParts();
                if (parts.size() > 2) {
                    throw new SemanticException(showTables, "too many parts in schema name: %s", schema);
                }
                if (parts.size() == 2) {
                    catalogName = parts.get(0);
                }
                schemaName = schema.getSuffix();
            }

            // TODO: throw SemanticException if schema does not exist

            Expression predicate = equal(nameReference("table_schema"), new StringLiteral(schemaName));

            String likePattern = showTables.getLikePattern();
            if (likePattern != null) {
                Expression likePredicate = new LikePredicate(nameReference("table_name"), new StringLiteral(likePattern), null);
                predicate = logicalAnd(predicate, likePredicate);
            }

            Query query = new Query(
                    selectList(aliasedName("table_name", "Table")),
                    table(QualifiedName.of(catalogName, INFORMATION_SCHEMA, TABLE_TABLES)),
                    predicate,
                    ImmutableList.<Expression>of(),
                    null,
                    ImmutableList.of(ascending("table_name")),
                    null);

            return visitQuery(query, context);
        }

        @Override
        protected AnalysisResult visitShowColumns(ShowColumns showColumns, AnalysisContext context)
        {
            QualifiedName table = showColumns.getTable();
            List<String> parts = Lists.reverse(table.getParts());
            if (parts.size() > 3) {
                throw new SemanticException(showColumns, "too many parts in table name: %s", table);
            }

            String catalogName = (parts.size() > 2) ? parts.get(2) : context.getSession().getCatalog();
            String schemaName = (parts.size() > 1) ? parts.get(1) : context.getSession().getSchema();
            String tableName = parts.get(0);

            // TODO: throw SemanticException if table does not exist
            Query query = new Query(
                    selectList(
                            aliasedName("column_name", "Column"),
                            aliasedName("data_type", "Type"),
                            aliasedName("is_nullable", "Null")),
                    table(QualifiedName.of(catalogName, INFORMATION_SCHEMA, TABLE_COLUMNS)),
                    logicalAnd(
                            equal(nameReference("table_schema"), new StringLiteral(schemaName)),
                            equal(nameReference("table_name"), new StringLiteral(tableName))),
                    ImmutableList.<Expression>of(),
                    null,
                    ImmutableList.of(ascending("ordinal_position")),
                    null);

            return visitQuery(query, context);
        }

        @Override
        protected AnalysisResult visitShowPartitions(ShowPartitions showPartitions, AnalysisContext context)
        {
            QualifiedName table = showPartitions.getTable();
            List<String> parts = Lists.reverse(table.getParts());
            if (parts.size() > 3) {
                throw new SemanticException(showPartitions, "too many parts in table name: %s", table);
            }

            String catalogName = (parts.size() > 2) ? parts.get(2) : context.getSession().getCatalog();
            String schemaName = (parts.size() > 1) ? parts.get(1) : context.getSession().getSchema();
            String tableName = parts.get(0);

            /*
                Generate a dynamic pivot to output one column per partition key.
                For example, a table with two partition keys (ds, cluster_name)
                would generate the following query:

                SELECT
                  max(CASE WHEN partition_key = 'ds' THEN partition_value END) ds
                , max(CASE WHEN partition_key = 'cluster_name' THEN partition_value END) cluster_name
                FROM ...
                GROUP BY partition_number
                ORDER BY partition_number
            */

            ImmutableList.Builder<Expression> selectList = ImmutableList.builder();
            for (String partition : metadata.listTablePartitionKeys(catalogName, schemaName, tableName)) {
                Expression key = equal(nameReference("partition_key"), new StringLiteral(partition));
                Expression function = functionCall("max", caseWhen(key, nameReference("partition_value")));
                selectList.add(new AliasedExpression(function, partition));
            }

            // TODO: throw SemanticException if table does not exist
            Query query = new Query(
                    selectAll(selectList.build()),
                    table(QualifiedName.of(catalogName, INFORMATION_SCHEMA, TABLE_INTERNAL_PARTITIONS)),
                    logicalAnd(
                            equal(nameReference("table_schema"), new StringLiteral(schemaName)),
                            equal(nameReference("table_name"), new StringLiteral(tableName))),
                    ImmutableList.of(nameReference("partition_number")),
                    null,
                    ImmutableList.of(ascending("partition_number")),
                    null);

            return visitQuery(query, context);
        }

        @Override
        protected AnalysisResult visitShowFunctions(ShowFunctions node, AnalysisContext context)
        {
            Query query = new Query(
                    selectList(
                            aliasedName("function_name", "Function"),
                            aliasedName("return_type", "Return Type"),
                            aliasedName("argument_types", "Argument Types")),
                    table(QualifiedName.of(INFORMATION_SCHEMA, TABLE_INTERNAL_FUNCTIONS)),
                    null,
                    ImmutableList.<Expression>of(),
                    null,
                    ImmutableList.of(ascending("function_name")),
                    null);

            return visitQuery(query, context);
        }

        private void analyzeDistinct(AnalyzedOutput output, List<AnalyzedOrdering> orderBy)
        {
            Set<Expression> outputExpressions = ImmutableSet.copyOf(Iterables.transform(output.getExpressions().values(), rewrittenExpressionGetter()));

            // get all order by expression that are not in the select clause
            List<AnalyzedOrdering> missingOrderings = IterableTransformer.on(orderBy)
                    .select(compose(not(in(outputExpressions)), Functions.compose(rewrittenExpressionGetter(), expressionGetter())))
                    .list();

            if (!missingOrderings.isEmpty()) {
                List<String> expressions = IterableTransformer.on(missingOrderings)
                        .transform(nodeGetter())
                        .transform(SortItem.sortKeyGetter())
                        .transform(ExpressionFormatter.expressionFormatterFunction())
                        .list();

                throw new SemanticException(missingOrderings.get(0).getNode(), "Expressions must appear in select list for SELECT DISTINCT, ORDER BY: %s", expressions);
            }
        }

        private List<AnalyzedOrdering> analyzeOrderBy(List<SortItem> orderBy, TupleDescriptor descriptor)
        {
            ImmutableList.Builder<AnalyzedOrdering> builder = ImmutableList.builder();
            for (SortItem sortItem : orderBy) {
                if (sortItem.getNullOrdering() != SortItem.NullOrdering.UNDEFINED) {
                    throw new SemanticException(sortItem, "Custom null ordering not yet supported");
                }

                AnalyzedExpression expression = new ExpressionAnalyzer(metadata).analyze(sortItem.getSortKey(), descriptor);
                builder.add(new AnalyzedOrdering(expression, sortItem.getOrdering(), sortItem));
            }

            return builder.build();
        }

        private AnalyzedExpression analyzePredicate(Expression predicate, TupleDescriptor sourceDescriptor)
        {
            // zeng: 解析expression, 获得expression的返回值type 以及 用symbol替换列全限定名
            AnalyzedExpression analyzedExpression = new ExpressionAnalyzer(metadata).analyze(predicate, sourceDescriptor);

            Type expressionType = analyzedExpression.getType();

            if (expressionType != Type.BOOLEAN && expressionType != Type.NULL) {
                throw new SemanticException(predicate, "WHERE clause must evaluate to a boolean: actual type %s", expressionType);
            }

            return analyzedExpression;
        }

        /**
         * Analyzes output expressions from select clause and expands wildcard selectors (e.g., SELECT * or SELECT T.*)
         */
        private AnalyzedOutput analyzeOutput(Select select, SymbolAllocator allocator, TupleDescriptor descriptor)
        {
            ImmutableList.Builder<Optional<String>> names = ImmutableList.builder();
            ImmutableList.Builder<Symbol> symbols = ImmutableList.builder();
            BiMap<Symbol, AnalyzedExpression> assignments = HashBiMap.create();
            ImmutableList.Builder<Type> types = ImmutableList.builder();

            for (Expression expression : select.getSelectItems()) {
                if (expression instanceof AllColumns) {
                    // expand * and T.*
                    Optional<QualifiedName> starPrefix = ((AllColumns) expression).getPrefix();
                    for (Field field : descriptor.getFields()) {
                        Optional<QualifiedName> prefix = field.getPrefix();
                        // Check if the prefix of the field name (i.e., the table name or relation alias) have a suffix matching the prefix of the wildcard
                        // e.g., SELECT T.* FROM S.T should resolve correctly
                        if (!starPrefix.isPresent() || prefix.isPresent() && prefix.get().hasSuffix(starPrefix.get())) {
                            names.add(field.getAttribute());
                            symbols.add(field.getSymbol());
                            types.add(field.getType());
                            assignments.put(field.getSymbol(), new AnalyzedExpression(field.getType(), new QualifiedNameReference(field.getSymbol().toQualifiedName())));
                        }
                    }
                }
                else {
                    Optional<String> alias = Optional.absent();
                    if (expression instanceof AliasedExpression) {
                        AliasedExpression aliased = (AliasedExpression) expression;

                        alias = Optional.of(aliased.getAlias());
                        expression = aliased.getExpression();
                    }
                    else if (expression instanceof QualifiedNameReference) {
                        alias = Optional.of(((QualifiedNameReference) expression).getName().getSuffix());
                    }

                    AnalyzedExpression analysis = new ExpressionAnalyzer(metadata).analyze(expression, descriptor);
                    Symbol symbol;
                    if (assignments.containsValue(analysis)) {
                        symbol = assignments.inverse().get(analysis);
                    }
                    else {
                        symbol = allocator.newSymbol(analysis.getRewrittenExpression(), analysis.getType());
                        assignments.put(symbol, analysis);
                    }

                    symbols.add(symbol);
                    names.add(alias);
                    types.add(analysis.getType());
                }
            }

            return new AnalyzedOutput(new TupleDescriptor(names.build(), symbols.build(), types.build()), assignments);
        }

        private List<AnalyzedExpression> analyzeGroupBy(List<Expression> groupBy, TupleDescriptor descriptor)
        {
            ImmutableList.Builder<AnalyzedExpression> builder = ImmutableList.builder();
            for (Expression expression : groupBy) {
                builder.add(new ExpressionAnalyzer(metadata).analyze(expression, descriptor));
            }
            return builder.build();
        }

        private Set<AnalyzedFunction> analyzeAggregations(List<Expression> groupBy, Select select, List<SortItem> orderBy, TupleDescriptor descriptor)
        {
            if (!groupBy.isEmpty() && Iterables.any(select.getSelectItems(), instanceOf(AllColumns.class))) {
                throw new SemanticException(select, "Wildcard selector not supported when GROUP BY is present"); // TODO: add support for SELECT T.*, count() ... GROUP BY T.* (maybe?)
            }

            List<Expression> scalarTerms = new ArrayList<>();
            ImmutableSet.Builder<AnalyzedFunction> aggregateTermsBuilder = ImmutableSet.builder();
            // analyze select and order by terms
            for (Expression term : concat(
                    transform(
                            select.getSelectItems(),
                            unaliasFunction()   // zeng: 去除别名
                    ),
                    transform(
                            orderBy,
                            sortKeyGetter() // zeng: 获取order by后面的expression
                    )
            )) {
                // TODO: this doesn't currently handle queries like 'SELECT k + sum(v) FROM T GROUP BY k' correctly
                AggregateAnalyzer analyzer = new AggregateAnalyzer(metadata, descriptor);

                // zeng: 分析expression获取 AnalyzedFunction
                List<AnalyzedFunction> aggregations = analyzer.analyze(term);

                if (aggregations.isEmpty()) {   // zeng: 非聚合列
                    scalarTerms.add(term);
                }
                else {  // zeng: 聚合列
                    if (Iterables.any(aggregations, distinctPredicate())) {
                        throw new SemanticException(select, "DISTINCT in aggregation parameters not yet supported");
                    }
                    aggregateTermsBuilder.addAll(aggregations);
                }
            }

            // zeng: 所有 AnalyzedFunction
            Set<AnalyzedFunction> aggregateTerms = aggregateTermsBuilder.build();

            if (!groupBy.isEmpty()) {
                // zeng: 非聚合列必须包含在group by中
                Iterable<Expression> notInGroupBy = Iterables.filter(scalarTerms, not(in(groupBy)));
                if (!Iterables.isEmpty(notInGroupBy)) {
                    throw new SemanticException(select, "Expressions must appear in GROUP BY clause or be used in an aggregate function: %s", Iterables.transform(notInGroupBy, ExpressionFormatter.expressionFormatterFunction()));
                }
            }
            else {
                // zeng: 没有group by时， 聚合列 和 非聚合列不能同时存在
                // if this is an aggregation query and some terms are not aggregates and there's no group by clause...
                if (!scalarTerms.isEmpty() && !aggregateTerms.isEmpty()) {
                    throw new SemanticException(select, "Mixing of aggregate and non-aggregate columns is illegal if there is no GROUP BY clause: %s", Iterables.transform(scalarTerms, ExpressionFormatter.expressionFormatterFunction()));
                }
            }

            // zeng: 返回所有 AnalyzedFunction
            return aggregateTerms;
        }
    }

    /**
     * Resolves and extracts aggregate functions from an expression and analyzes them (infer types and replace QualifiedNames with symbols)
     */
    private static class AggregateAnalyzer
            extends DefaultTraversalVisitor<Void, FunctionCall>
    {
        private final Metadata metadata;
        private final TupleDescriptor descriptor;

        private List<AnalyzedFunction> aggregations;

        public AggregateAnalyzer(Metadata metadata, TupleDescriptor descriptor)
        {
            this.metadata = metadata;
            this.descriptor = descriptor;
        }

        public List<AnalyzedFunction> analyze(Expression expression)
        {
            // zeng: reset
            aggregations = new ArrayList<>();
            // zeng: 解析expression节点获取AnalyzedFunction, 表达式中可能包含FunctionCall节点
            // zeng: 一个表达式可能包含多个FunctionCall节点
            process(expression, null);

            return aggregations;
        }

        @Override
        protected Void visitFunctionCall(FunctionCall node, FunctionCall enclosingAggregate)    // zeng: FunctionCall节点
        {
            ImmutableList.Builder<AnalyzedExpression> argumentsAnalysis = ImmutableList.builder();
            ImmutableList.Builder<Type> argumentTypes = ImmutableList.builder();

            for (Expression expression : node.getArguments()) { // zeng: 遍历参数
                // zeng: 解析参数expression为AnalyzedExpression
                AnalyzedExpression analysis = new ExpressionAnalyzer(metadata).analyze(expression, descriptor);
                argumentsAnalysis.add(analysis);

                // zeng: 参数值类型
                argumentTypes.add(analysis.getType());
            }

            // zeng: NativeMetadata.getFunction 因为聚合是presto本身在做
            FunctionInfo info = metadata.getFunction(node.getName(), Lists.transform(argumentTypes.build(), Type.toRaw()));

            if (info != null && info.isAggregate()) {
                // zeng: 不支持嵌套聚合函数
                if (enclosingAggregate != null) {
                    throw new SemanticException(node, "Cannot nest aggregate functions: %s", ExpressionFormatter.toString(enclosingAggregate));
                }

                // zeng: 列 全限定名 改写为 symbol
                FunctionCall rewritten = TreeRewriter.rewriteWith(new NameToSymbolRewriter(descriptor), node);

                aggregations.add(
                        // zeng: function info，解析过的参数expression， 改写列全限定名为symbol的function call, 参数中是否包含distinct
                        new AnalyzedFunction(info, argumentsAnalysis.build(), rewritten, node.isDistinct())
                );

                // zeng: 参数中可能有非聚合函数
                return super.visitFunctionCall(node, node); // visit children
            }

            // zeng: 非聚合函数的参数可以是聚合函数
            return super.visitFunctionCall(node, enclosingAggregate);
        }
    }

    private static Function<Expression, Expression> unaliasFunction()
    {
        return new Function<Expression, Expression>()
        {
            @Override
            public Expression apply(Expression input)
            {
                if (input instanceof AliasedExpression) {
                    return ((AliasedExpression) input).getExpression();
                }

                return input;
            }
        };
    }

    private static class RelationAnalyzer
            extends DefaultTraversalVisitor<TupleDescriptor, AnalysisContext>
    {
        private final Metadata metadata;
        private final Session session;

        private RelationAnalyzer(Metadata metadata, Session session)
        {
            this.metadata = metadata;
            this.session = session;
        }

        @Override
        protected TupleDescriptor visitTable(Table table, AnalysisContext context)
        {
            // zeng: 全限定名
            QualifiedName name = table.getName();

            if (name.getParts().size() > 3) {
                throw new SemanticException(table, "Too many dots in table name: %s", name);
            }

            List<String> parts = Lists.reverse(name.getParts());

            // zeng: 表名
            String tableName = parts.get(0);
            // zeng: schema名称
            String schemaName = (parts.size() > 1) ? parts.get(1) : session.getSchema();
            // zeng: catalog名
            String catalogName = (parts.size() > 2) ? parts.get(2) : session.getCatalog();

            // zeng: TODO 获取table元数据
            TableMetadata tableMetadata = metadata.getTable(catalogName, schemaName, tableName);

            if (tableMetadata == null) {
                throw new SemanticException(table, "Table %s does not exist", name);
            }

            // zeng: 组装列
            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                // zeng: catalog, schema, table name 组成的限定名
                QualifiedName prefix = QualifiedName.of(tableMetadata.getCatalogName(), tableMetadata.getSchemaName(), tableMetadata.getTableName());

                // zeng: TODO 列在查询结果中的唯一标识符？
                Symbol symbol = context.getSymbolAllocator().newSymbol(column.getName(), Type.fromRaw(column.getType()));

                Preconditions.checkArgument(column.getColumnHandle().isPresent(), "Column doesn't have a handle");

                // zeng: new Field
                fields.add(new Field(Optional.of(prefix), Optional.of(column.getName()), column.getColumnHandle(), symbol, Type.fromRaw(column.getType())));
            }

            // zeng: 所有列
            TupleDescriptor descriptor = new TupleDescriptor(fields.build());

            // zeng: 表元数据 和 列的列表 存入context
            context.registerTable(table, descriptor, tableMetadata);

            return descriptor;
        }

        // zeng: TODO
        @Override
        protected TupleDescriptor visitAliasedRelation(AliasedRelation relation, AnalysisContext context)
        {
            if (relation.getColumnNames() != null && !relation.getColumnNames().isEmpty()) {
                throw new UnsupportedOperationException("not yet implemented: column mappings in relation alias");
            }

            TupleDescriptor child = process(relation.getRelation(), context);

            ImmutableList.Builder<Field> builder = ImmutableList.builder();
            for (Field field : child.getFields()) {
                builder.add(new Field(Optional.of(QualifiedName.of(relation.getAlias())), field.getAttribute(), field.getColumn(), field.getSymbol(), field.getType()));
            }

            return new TupleDescriptor(builder.build());
        }

        // zeng: TODO
        @Override
        protected TupleDescriptor visitSubquery(Subquery node, AnalysisContext context)
        {
            // Analyze the subquery recursively
            AnalysisResult analysis = new Analyzer(context.getSession(), metadata).analyze(node.getQuery(), new AnalysisContext(context.getSession(), context.getSymbolAllocator()));

            context.registerInlineView(node, analysis);

            return analysis.getOutputDescriptor();
        }

        // zeng: TODO
        @Override
        protected TupleDescriptor visitJoin(Join node, AnalysisContext context)
        {
            if (node.getType() != Join.Type.INNER) {
                throw new SemanticException(node, "Only inner joins are supported");
            }

            TupleDescriptor leftTuple = process(node.getLeft(), context);
            TupleDescriptor rightTuple = process(node.getRight(), context);

            TupleDescriptor descriptor = new TupleDescriptor(ImmutableList.copyOf(Iterables.concat(leftTuple.getFields(), rightTuple.getFields())));

            JoinCriteria criteria = node.getCriteria();
            if (criteria instanceof NaturalJoin) {
                throw new SemanticException(node, "Natural join not supported");
            }
            else if (criteria instanceof JoinUsing) {
                // TODO: implement proper "using" semantics with respect to output columns
                List<String> columns = ((JoinUsing) criteria).getColumns();

                ImmutableList.Builder<AnalyzedJoinClause> clauses = ImmutableList.builder();
                for (String column : columns) {
                    AnalyzedExpression left = new ExpressionAnalyzer(metadata).analyze(new QualifiedNameReference(new QualifiedName(column)), leftTuple);
                    AnalyzedExpression right = new ExpressionAnalyzer(metadata).analyze(new QualifiedNameReference(new QualifiedName(column)), rightTuple);

                    clauses.add(new AnalyzedJoinClause(left, right));
                }
                context.registerJoin(node, clauses.build());
                return descriptor;
            }
            else if (criteria instanceof JoinOn) {
                // verify the types are correct, etc
                new ExpressionAnalyzer(metadata).analyze(((JoinOn) criteria).getExpression(), descriptor);

                ImmutableList.Builder<AnalyzedJoinClause> clauses = ImmutableList.builder();
                Expression expression = ((JoinOn) criteria).getExpression();

                Object optimizedExpression = ExpressionInterpreter.expressionOptimizer(new NoOpSymbolResolver(), metadata, session).process(expression, null);

                if (!(optimizedExpression instanceof Expression)) {
                    throw new SemanticException(node, "Joins on constant expressions (i.e., cross joins) not supported");
                }

                for (Expression conjunct : ExpressionUtils.extractConjuncts((Expression) optimizedExpression)) {
                    if (!(conjunct instanceof ComparisonExpression)) {
                        throw new SemanticException(node, "Non-equi joins not supported");
                    }

                    ComparisonExpression comparison = (ComparisonExpression) conjunct;
                    if (comparison.getType() != ComparisonExpression.Type.EQUAL) {
                        throw new SemanticException(node, "Non-equi joins not supported");
                    }

                    AnalyzedExpression first = new ExpressionAnalyzer(metadata).analyze(comparison.getLeft(), descriptor);
                    Set<Symbol> firstDependencies = DependencyExtractor.extract(first.getRewrittenExpression());

                    AnalyzedExpression second = new ExpressionAnalyzer(metadata).analyze(comparison.getRight(), descriptor);
                    Set<Symbol> secondDependencies = DependencyExtractor.extract(second.getRewrittenExpression());

                    Set<Symbol> leftSymbols = leftTuple.getSymbols().keySet();
                    Set<Symbol> rightSymbols = rightTuple.getSymbols().keySet();

                    AnalyzedExpression leftExpression;
                    AnalyzedExpression rightExpression;
                    if (leftSymbols.containsAll(firstDependencies) && rightSymbols.containsAll(secondDependencies)) {
                        leftExpression = first;
                        rightExpression = second;
                    }
                    else if (leftSymbols.containsAll(secondDependencies) && rightSymbols.containsAll(firstDependencies)) {
                        leftExpression = second;
                        rightExpression = first;
                    }
                    else {
                        // must have a complex expression that involves both tuples on one side of the comparison expression (e.g., coalesce(left.x, right.x) = 1)
                        throw new SemanticException(node, "Non-equi joins not supported");
                    }

                    clauses.add(new AnalyzedJoinClause(leftExpression, rightExpression));
                }

                context.registerJoin(node, clauses.build());
                return descriptor;
            }
            else {
                throw new UnsupportedOperationException("unsupported join criteria: " + criteria.getClass().getName());
            }
        }
    }

    private static class NoOpSymbolResolver
            implements SymbolResolver
    {
        @Override
        public Object getValue(Symbol symbol)
        {
            return new QualifiedNameReference(symbol.toQualifiedName());
        }
    }
}
