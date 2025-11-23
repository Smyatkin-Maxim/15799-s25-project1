package edu.cmu.cs.db.calcite_app.app;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.interpreter.Bindables.BindableTableScanRule;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect.DatabaseProduct;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelRunner;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;

import com.google.common.collect.ImmutableList;

public class App {
    static class QueryWithTimeout implements Callable<ResultSet> {
        private PreparedStatement stmt;

        QueryWithTimeout(PreparedStatement stmt) {
            this.stmt = stmt;
        }

        @Override
        public ResultSet call() throws Exception {
            return stmt.executeQuery();
        }
    }

    static class OptimizeWithTimeout implements Callable<RelNode> {
        private VolcanoPlanner p;
        private RelNode unoptimized;
        private RelOptCluster cluster;

        OptimizeWithTimeout(VolcanoPlanner planner, RelNode unoptimized, RelOptCluster cluster) {
            this.p = planner;
            this.unoptimized = unoptimized;
            this.cluster = cluster;
        }

        @Override
        public RelNode call() {
            Program program = Programs.of(RuleSets.ofList(p.getRules()));
            RelTraitSet toTraits = unoptimized.getTraitSet()
                    .replace(EnumerableConvention.INSTANCE);
            resetMDProviders();
            unoptimized.getCluster().setMetadataProvider(cluster.getMetadataProvider());
            return program.run(p, unoptimized, toTraits,
                    ImmutableList.of(), ImmutableList.of());
        }
    }

    private static RelOptCluster cluster;
    private static SqlToRelConverter converter;
    private static CalciteConnection conn;
    private static Schema schema;
    private static SqlValidator validator;
    private static VolcanoPlanner planner;
    private static CalciteConnectionConfig connConfig;
    private static Prepare.CatalogReader catalogReader;
    private static Map<String, List<Double>> execTime;
    private static Connection duckConn;
    private final static Boolean runInDuck = false;

    private static void AddEnumerableRules() {
        planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);
    }

    private static void SerializePlan(RelNode relNode, File outputPath) throws IOException {
        Files.writeString(outputPath.toPath(),
                RelOptUtil.dumpPlan("", relNode, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));
    }

    private static void SerializeResultSet(ResultSet resultSet, File outputPath) throws SQLException, IOException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        StringBuilder resultSetString = new StringBuilder();
        for (int i = 1; i <= columnCount; i++) {
            if (i > 1) {
                resultSetString.append(",");
            }
            resultSetString.append(metaData.getColumnName(i));
        }
        resultSetString.append("\n");
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) {
                    resultSetString.append(",");
                }
                String s = resultSet.getString(i);
                s = s.replace("\n", "\\n");
                s = s.replace("\r", "\\r");
                s = s.replace("\"", "\"\"");
                resultSetString.append("\"");
                resultSetString.append(s);
                resultSetString.append("\"");
            }
            resultSetString.append("\n");
        }
        Files.writeString(outputPath.toPath(), resultSetString.toString());
    }

    private static void WritePerformanceReport(File outputPath) throws Exception {
        StringBuilder performanceString = new StringBuilder();
        for (String q : execTime.keySet()) {
            Double sum = 0.0;
            for (Double val : execTime.get(q)) {
                sum += val;
            }
            performanceString.append(q + " " + ((Double) (sum / execTime.get(q).size())).toString() + "\n");
        }
        Files.writeString(outputPath.toPath(), performanceString.toString());
    }

    private static void createSchema() throws Exception {
        Properties info = new Properties();
        info.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        conn = DriverManager.getConnection("jdbc:calcite:", info).unwrap(CalciteConnection.class);
        App.schema = conn.getRootSchema();
        Class.forName("org.duckdb.DuckDBDriver");
        Properties ro_prop = new Properties();
        ro_prop.setProperty("duckdb.read_only", "true");
        duckConn = DriverManager.getConnection("jdbc:duckdb:../items.db", ro_prop);
        conn.getRootSchema().add("main", new DuckDBSchema(duckConn));
    }

    private static SqlNode parseSql(String sql) throws Exception {
        SqlParser.Config config = SqlParser.Config.DEFAULT;
        config.withCaseSensitive(false);
        SqlParser parser = SqlParser.create(sql, config);
        return parser.parseStmt();
    }

    private static void createValidator() throws Exception {
        Properties configProperties = new Properties();
        configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.FALSE.toString());
        configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        configProperties.put(CalciteConnectionProperty.FORCE_DECORRELATE.camelName(), Boolean.TRUE.toString());

        connConfig = new CalciteConnectionConfigImpl(configProperties);

        RelDataTypeFactory typeFactory = conn.getTypeFactory();
        catalogReader = new CalciteCatalogReader(
                CalciteSchema.from((SchemaPlus) schema),
                Collections.singletonList("main"),
                typeFactory,
                connConfig);

        SqlOperatorTable operatorTable = SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                SqlLibrary.STANDARD, SqlLibrary.POSTGRESQL);

        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(connConfig.lenientOperatorLookup())
                .withDefaultNullCollation(connConfig.defaultNullCollation())
                .withIdentifierExpansion(true);

        App.validator = SqlValidatorUtil.newValidator(
                operatorTable,
                catalogReader,
                typeFactory,
                validatorConfig);
    }

    private static void createPlanner() {
        App.planner = new VolcanoPlanner(
                null,
                Contexts.of(connConfig));
        App.planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    }

    private static void resetMDProviders() {
        MyNdvProvider myNdvProvider = new MyNdvProvider();
        RelMetadataProvider customNdvProvider = ReflectiveRelMetadataProvider.reflectiveSource(
                myNdvProvider,
                BuiltInMetadata.DistinctRowCount.Handler.class // Explicitly state the handler interface
        );
        MySelectivityProvider mySelectivityProvider = new MySelectivityProvider();
        RelMetadataProvider customSelectivityProvider = ReflectiveRelMetadataProvider.reflectiveSource(
                mySelectivityProvider,
                BuiltInMetadata.Selectivity.Handler.class // Explicitly state the handler interface
        );
        MyRowCountProvider myRcProvider = new MyRowCountProvider();
        RelMetadataProvider customRcProvider = ReflectiveRelMetadataProvider.reflectiveSource(myRcProvider,
                BuiltInMetadata.RowCount.Handler.class);
        RelMetadataProvider chainedProvider = ChainedRelMetadataProvider.of(
                ImmutableList.of(
                        customNdvProvider,
                        customSelectivityProvider,
                        customRcProvider,
                        DefaultRelMetadataProvider.INSTANCE));
        cluster.setMetadataProvider(chainedProvider);
    }

    private static void createCluster() {
        cluster = RelOptCluster.create(
                App.planner,
                new RexBuilder(conn.getTypeFactory()));
        resetMDProviders();
    }

    private static void createRelConverter() {
        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config();
        converterConfig.withTrimUnusedFields(true);
        converterConfig.withExpand(false);

        converter = new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig);
    }

    private static RelNode convertToRel(SqlNode validatedNode) {
        return converter.convertQuery(validatedNode, false, true).rel;
    }

    private static RelNode runHepPhase(RelNode original, Boolean bushy) {
        resetMDProviders();
        original.getCluster().setMetadataProvider(cluster.getMetadataProvider());
        HepProgramBuilder builder = HepProgram.builder();
        builder.addRuleInstance(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE)
                .addRuleInstance(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE)
                .addRuleInstance(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE)
                .addRuleInstance(FilterPullFactorsRule.Config.DEFAULT.toRule());
        if (bushy) {
            builder.addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN);
        }
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        HepProgram hepProgram = builder.build();
        Program program = Programs.of(hepProgram, true, cluster.getMetadataProvider());
        RelNode corellated = program.run(cluster.getPlanner(), original, cluster.traitSet(),
                Collections.emptyList(), Collections.emptyList());
        // HEP silently overwrites whatever metadata providers you give
        // What a mess
        resetMDProviders();
        return RelDecorrelator.decorrelateQuery(corellated);
    }

    private static RelNode runVolcanoPhase(RelNode original, Boolean bushy) throws Exception {
        planner.clear();
        AddEnumerableRules();

        if (!bushy) {
            // For whatever reason (perhaps, cardinality misestimates) Calcite likes to pick
            // poor plans with cartesian product. So I turn them off here
            planner.addRule(CoreRules.JOIN_COMMUTE.config
                    .withAllowAlwaysTrueCondition(false)
                    .withSwapOuter(true)
                    .toRule());
            planner.addRule(CoreRules.JOIN_ASSOCIATE.config
                    .withAllowAlwaysTrueCondition(false)
                    .toRule());
            planner.addRule(JoinPushThroughJoinRule.RIGHT);
            planner.addRule(JoinPushThroughJoinRule.LEFT);
        } else {
            planner.addRule(CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY);
            planner.addRule(CoreRules.MULTI_JOIN_BOTH_PROJECT);
            planner.addRule(CoreRules.FILTER_MULTI_JOIN_MERGE);
            planner.addRule(CoreRules.PROJECT_MULTI_JOIN_MERGE);
            planner.addRule(CoreRules.MULTI_JOIN_BOTH_PROJECT);
        }

        planner.addRule(CoreRules.FILTER_INTO_JOIN);
        planner.addRule(CoreRules.JOIN_CONDITION_PUSH);
        planner.addRule(CoreRules.JOIN_EXTRACT_FILTER);
        // q20, q17
        // WTF_MOMENT: This rule gives better plan and better runtimes on my tests.
        // But on gradescope it degenerates performance to score 0
        // planner.addRule(CoreRules.JOIN_DERIVE_IS_NOT_NULL_FILTER_RULE);

        planner.addRule(CoreRules.FILTER_AGGREGATE_TRANSPOSE);
        planner.addRule(CoreRules.FILTER_PROJECT_TRANSPOSE);
        planner.addRule(CoreRules.PROJECT_JOIN_TRANSPOSE);
        planner.addRule(CoreRules.PROJECT_AGGREGATE_MERGE);

        // This bunch of rules removes some rediculous stuff that never
        // is supposed to be executed. Seems to be working only for cap1
        // query and doesn't give any additional points, but nevertheless
        // I'll keep it here
        planner.addRule(PruneEmptyRules.PROJECT_INSTANCE);
        planner.addRule(PruneEmptyRules.AGGREGATE_INSTANCE);
        planner.addRule(PruneEmptyRules.EMPTY_TABLE_INSTANCE);
        planner.addRule(PruneEmptyRules.FILTER_INSTANCE);
        planner.addRule(PruneEmptyRules.JOIN_LEFT_INSTANCE);
        planner.addRule(PruneEmptyRules.JOIN_RIGHT_INSTANCE);
        planner.addRule(PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE);

        // Lots of queries fail without this one due to not implemented AVG.
        // It also helps to merge common aggregates:
        // e.g., avg(x), count(x), sum(x) would be expanded to
        // sum(x)/count(x), count(x), sum(x),
        // meaning that only 2 aggregate functions really need to be calculated
        planner.addRule(CoreRules.AGGREGATE_REDUCE_FUNCTIONS);

        // q16 and capybara3.sql. I'm not exactly sure why count distinct is a problem,
        // perhaps it simply has to be implemented. But instead we rewrite it to a join
        // with group by
        planner.addRule(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN);

        // evaluate the most promising conditions first
        // planner.addRule(FilterReorderRule.Config.DEFAULT.toRule());

        resetMDProviders();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<RelNode> future = executor.submit(new OptimizeWithTimeout(planner, original, cluster));
        executor.shutdown();
        try {
            return future.get(20, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            System.out.println("Full join ordering timed out. Trying a more narrow search");
            throw e;
        } catch (Exception e) {
            System.out.println("Optimization failure");
            throw e;
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private static RelNode optimize(RelNode unoptimizedRelNode) throws Exception {
        RelNode optimizedBushy = null, optimized = null;
        try {
            optimizedBushy = runVolcanoPhase(runHepPhase(unoptimizedRelNode, true), true);
        } catch (Exception e) {
        }
        try {
            optimized = runVolcanoPhase(runHepPhase(unoptimizedRelNode, false), false);
        } catch (Exception e) {
        }

        if (optimizedBushy == null) {
            return optimized;
        } else if (optimized == null) {
            System.out.println("Picking bushy!");
            return optimizedBushy;
        }
        RelOptCost costBushy = cluster.getMetadataQuery().getCumulativeCost(optimizedBushy),
                costNoBushy = cluster.getMetadataQuery().getCumulativeCost(optimized);
        System.out.println(costBushy);
        System.out.println(costNoBushy);
        if (costNoBushy.isLe(costBushy)) {
            return optimized;
        } else {
            System.out.println("Picking bushy!");
            return optimizedBushy;
        }
    }

    private static ResultSet executeDuckDBQuery(String query) throws Exception {
        return duckConn.createStatement().executeQuery(query);
    }

    private static ResultSet executeCalciteQuery(RelNode optimized) throws Exception {
        RelRunner runner = conn.unwrap(RelRunner.class);
        PreparedStatement stmt = runner.prepareStatement(optimized);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<ResultSet> future = executor.submit(new QueryWithTimeout(stmt));
        executor.shutdownNow();
        try {
            return future.get(120, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw e;
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private static void runQuery(File inPath, File outPath, String queryFile, Set<String> skiplist) throws Exception {
        String filename = inPath.getName().split("\\.")[0];
        if (queryFile != null && !filename.equals(queryFile)) {
            return;
        }
        if (skiplist.contains(filename)) {
            return;
        }
        App.execTime.putIfAbsent(filename, new ArrayList<Double>());
        System.out.println("Trying " + filename);
        String rawQuery = String.join("\n", Files.readAllLines(inPath.toPath()));
        Files.writeString(Paths.get(outPath.toString(), inPath.getName()), rawQuery);
        SqlNode parsedNode = parseSql(rawQuery);
        SqlNode validated = validator.validate(parsedNode);
        RelNode relNode = convertToRel(validated);
        SerializePlan(relNode, Paths.get(outPath.toString(), filename + ".txt").toFile());
        RelNode optimized = optimize(relNode);
        SerializePlan(optimized, Paths.get(outPath.toString(), filename + "_optimized.txt").toFile());

        planner.clear();
        AddEnumerableRules();

        SqlNode optimizedSqlNode = new RelToSqlConverter(
                DatabaseProduct.DUCKDB.getDialect()).visitRoot(optimized).asStatement();
        String optimizedQueryText = optimizedSqlNode.toSqlString(DatabaseProduct.DUCKDB.getDialect()).toString();
        Files.writeString(Paths.get(outPath.toString(), filename + "_optimized.sql"), optimizedQueryText);

        long start = System.currentTimeMillis();

        try {
            ResultSet rs = runInDuck ? executeDuckDBQuery(optimizedQueryText) : executeCalciteQuery(optimized);
            String execTime = new Double(System.currentTimeMillis() - start).toString();
            App.execTime.get(filename).add(new Double(execTime));
            System.out.println(filename + " finished successfully in " + execTime + " ms");
            SerializeResultSet(rs, Paths.get(outPath.toString(), filename + "_results.csv").toFile());
        } catch (TimeoutException e) {
            App.execTime.get(filename).add(new Double(99999));
        } catch (Exception e) {
            Files.writeString(Paths.get(outPath.toString(), filename + "_results.csv"), e.getMessage());
            App.execTime.get(filename).add(new Double(99999));
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: java -jar App.jar <arg1> <arg2>");
            return;
        }

        System.out.println("Running the app!");
        String in_path = args[0];
        System.out.println("\tArg1: " + in_path);
        String out_path = args[1];
        System.out.println("\tArg2: " + out_path);
        String queryFile = null;
        if (args.length == 3) {
            queryFile = args[2];
        }

        System.out.println("Running queries");
        File dir = new File(in_path);
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".sql");
            }
        });
        createSchema();
        createValidator();
        createPlanner();
        createCluster();
        createRelConverter();
        App.execTime = new HashMap<java.lang.String, List<Double>>();

        Set<String> skiplist = new HashSet<String>();
        int n_runs = 5;
        duckConn.createStatement().execute("PRAGMA disable_optimizer;");
        for (int i = 0; i < n_runs; ++i) {
            for (File sqlfile : files) {
                try {
                    runQuery(sqlfile, new File(out_path), queryFile, skiplist);
                } catch (Exception e) {
                    System.err.println(sqlfile.getName() + " failed");
                    System.err.println(e.getMessage());
                }
            }
        }
        execTime.forEach((key, value) -> System.out.println(key + " " + value));
        WritePerformanceReport(new File("../scores.txt"));
    }
}
