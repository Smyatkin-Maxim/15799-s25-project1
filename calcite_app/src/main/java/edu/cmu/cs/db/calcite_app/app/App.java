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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.CoreRules;
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
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelRunner;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import com.google.common.collect.ImmutableList;

public class App {
    static class Task implements Callable<ResultSet> {
        private PreparedStatement stmt;

        Task(PreparedStatement stmt) {
            this.stmt = stmt;
        }

        @Override
        public ResultSet call() throws Exception {
            return stmt.executeQuery();
        }
    }

    private static RelOptCluster cluster;
    private static CalciteConnection conn;
    private static Schema schema;
    private static SqlValidator validator;
    private static VolcanoPlanner planner;
    private static CalciteConnectionConfig connConfig;
    private static Prepare.CatalogReader catalogReader;
    private static Map<String, String> execTime;

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

    private static void createSchema() throws Exception {
        Properties info = new Properties();
        info.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        conn = DriverManager.getConnection("jdbc:calcite:", info).unwrap(CalciteConnection.class);
        App.schema = conn.getRootSchema();
        conn.getRootSchema().add("main", new DuckDBSchema());
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
                RelOptCostImpl.FACTORY,
                Contexts.of(connConfig));
        App.planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    }

    private static RelNode convertToRel(SqlNode validatedNode) {
        cluster = RelOptCluster.create(
                App.planner,
                new RexBuilder(conn.getTypeFactory()));
        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config();
        converterConfig.withTrimUnusedFields(true);
        converterConfig.withExpand(false);

        SqlToRelConverter converter = new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig);

        return converter.convertQuery(validatedNode, false, true).rel;
    }

    private static RelNode decorellate(RelNode original) {
        HepProgram hepProgram = HepProgram.builder()
                .addRuleCollection(
                        ImmutableList.of(
                                // SubQuery program rules
                                CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
                                CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
                                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE,
                                // plus FilterAggregateTransposeRule
                                CoreRules.FILTER_AGGREGATE_TRANSPOSE,
                                CoreRules.FILTER_PROJECT_TRANSPOSE))
                .build();
        Program program = Programs.of(hepProgram, true, cluster.getMetadataProvider());
        RelNode corellated = program.run(cluster.getPlanner(), original, cluster.traitSet(),
                Collections.emptyList(), Collections.emptyList());
        return RelDecorrelator.decorrelateQuery(corellated);
    }

    private static RelNode optimize(RelNode unoptimizedRelNode) {
        unoptimizedRelNode = decorellate(unoptimizedRelNode);

        // join rules
        planner.addRule(CoreRules.FILTER_INTO_JOIN);
        planner.addRule(CoreRules.JOIN_EXTRACT_FILTER);
        planner.addRule(CoreRules.JOIN_COMMUTE);
        planner.addRule(CoreRules.JOIN_ASSOCIATE);
        planner.addRule(CoreRules.AGGREGATE_PROJECT_MERGE);
        planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);

        // planner.addRule(CoreRules.AGGREGATE_FILTER_TRANSPOSE);
        // planner.addRule(CoreRules.FILTER_PROJECT_TRANSPOSE);
        // planner.addRule(CoreRules.FILTER_CORRELATE);
        // planner.addRule(CoreRules.PROJECT_TO_CALC);
        // planner.addRule(CoreRules.FILTER_TO_CALC);

        planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_CORRELATE_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE);
        Program program = Programs.of(RuleSets.ofList(planner.getRules()));
        RelTraitSet toTraits = unoptimizedRelNode.getTraitSet()
                .replace(EnumerableConvention.INSTANCE);
        return program.run(planner, unoptimizedRelNode, toTraits,
                ImmutableList.of(), ImmutableList.of());
    }

    private static void runQuery(File inPath, File outPath) throws Exception {
        String filename = inPath.getName().split("\\.")[0];
        App.execTime.put(filename, "failure");
        System.out.println("Trying " + filename);
        String rawQuery = String.join("\n", Files.readAllLines(inPath.toPath()));
        Files.writeString(Paths.get(outPath.toString(), inPath.getName()), rawQuery);
        SqlNode parsedNode = parseSql(rawQuery);
        SqlNode validated = validator.validate(parsedNode);
        RelNode relNode = convertToRel(validated);
        SerializePlan(relNode, Paths.get(outPath.toString(), filename + ".txt").toFile());
        RelNode optimized = optimize(relNode);
        SerializePlan(optimized, Paths.get(outPath.toString(), filename + "_optimized.txt").toFile());
        RelRunner runner = conn.unwrap(RelRunner.class);
        PreparedStatement stmt = runner.prepareStatement(optimized);
        long start = System.currentTimeMillis();
        SqlNode optimizedSqlNode = new RelToSqlConverter(
                DatabaseProduct.DUCKDB.getDialect()).visitRoot(optimized).asStatement();
        Files.writeString(Paths.get(outPath.toString(), filename + "_optimized.sql"),
                optimizedSqlNode.toSqlString(DatabaseProduct.DUCKDB.getDialect()).toString());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<ResultSet> future = executor.submit(new Task(stmt));
        ResultSet rs;
        try {
            rs = future.get(45, TimeUnit.SECONDS);
            String execTime = new Double(System.currentTimeMillis() - start).toString();
            App.execTime.put(filename, execTime + " ms");
            System.out.println(filename + " finished successfully in " + execTime + " ms");
            SerializeResultSet(rs, Paths.get(outPath.toString(), filename + "_results.txt").toFile());
        } catch (TimeoutException e) {
            future.cancel(true);
            App.execTime.put(filename, "timeout");
        } finally {
            executor.shutdownNow();
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
        App.execTime = new HashMap<java.lang.String, java.lang.String>();

        for (File sqlfile : files) {
            try {
                runQuery(sqlfile, new File(out_path));
            } catch (Exception e) {
                System.err.println(sqlfile.getName() + " failed");
                System.err.println(e.getMessage());
            }
        }
        execTime.forEach((key, value) -> System.out.println(key + " " + value));
    }
}
