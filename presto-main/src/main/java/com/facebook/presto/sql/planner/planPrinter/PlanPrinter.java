/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.planPrinter;

import com.facebook.presto.Session;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.PlanNodeCost;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.StageStats;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.OperatorNotFoundException;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.operator.HashCollisionsInfo;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.PipelineStats;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.operator.WindowOperator.IndexInfo;
import com.facebook.presto.operator.WindowOperator.SingleDriverWindowInfo;
import com.facebook.presto.operator.WindowOperator.WindowInfo;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.FunctionInvoker;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExceptNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode.Scope;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.IntersectNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.MetadataDeleteNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.facebook.presto.util.GraphvizPrinter;
import com.google.common.base.CaseFormat;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import io.airlift.stats.Distribution;
import io.airlift.stats.Distribution.DistributionSnapshot;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.cost.PlanNodeCost.UNKNOWN_COST;
import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.operator.scalar.MathFunctions.sqrt;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.DomainUtils.simplifyDomain;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Lists.reverse;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.DataSize.succinctDataSize;
import static java.lang.Double.isFinite;
import static java.lang.Double.max;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class PlanPrinter
{
    private final StringBuilder output = new StringBuilder();
    private final Metadata metadata;
    private final Optional<Map<PlanNodeId, PlanNodeStats>> stats;

    private PlanPrinter(PlanNode plan, Map<Symbol, Type> types, Metadata metadata, CostCalculator costCalculator, Session sesion)
    {
        this(plan, types, metadata, costCalculator, sesion, 0);
    }

    private PlanPrinter(PlanNode plan, Map<Symbol, Type> types, Metadata metadata, CostCalculator costCalculator, Session session, int indent)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(types, "types is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(costCalculator, "costCalculator is null");

        this.metadata = metadata;
        this.stats = Optional.empty();

        Map<PlanNode, PlanNodeCost> costs = costCalculator.calculateCostForPlan(session, types, plan);
        Visitor visitor = new Visitor(types, costs, session);
        plan.accept(visitor, indent);
    }

    private PlanPrinter(PlanNode plan, Map<Symbol, Type> types, Metadata metadata, CostCalculator costCalculator, Session session, Map<PlanNodeId, PlanNodeStats> stats, int indent)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(types, "types is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(costCalculator, "costCalculator is null");

        this.metadata = metadata;
        this.stats = Optional.of(stats);

        Map<PlanNode, PlanNodeCost> costs = costCalculator.calculateCostForPlan(session, types, plan);
        Visitor visitor = new Visitor(types, costs, session);
        plan.accept(visitor, indent);
    }

    @Override
    public String toString()
    {
        return output.toString();
    }

    public static String textLogicalPlan(PlanNode plan, Map<Symbol, Type> types, Metadata metadata, CostCalculator costCalculator, Session session)
    {
        return new PlanPrinter(plan, types, metadata, costCalculator, session).toString();
    }

    public static String textLogicalPlan(PlanNode plan, Map<Symbol, Type> types, Metadata metadata, CostCalculator costCalculator, Session session, int indent)
    {
        return new PlanPrinter(plan, types, metadata, costCalculator, session, indent).toString();
    }

    public static String textLogicalPlan(PlanNode plan, Map<Symbol, Type> types, Metadata metadata, CostCalculator costCalculator, Session session, Map<PlanNodeId, PlanNodeStats> stats, int indent)
    {
        return new PlanPrinter(plan, types, metadata, costCalculator, session, stats, indent).toString();
    }

    public static String textDistributedPlan(StageInfo outputStageInfo, Metadata metadata, CostCalculator costCalculator, Session session)
    {
        StringBuilder builder = new StringBuilder();
        List<StageInfo> allStages = outputStageInfo.getSubStages().stream()
                .flatMap(stage -> getAllStages(Optional.of(stage)).stream())
                .collect(toImmutableList());
        for (StageInfo stageInfo : allStages) {
            Map<PlanNodeId, PlanNodeStats> aggregatedStats = aggregatePlanNodeStats(stageInfo);
            builder.append(formatFragment(metadata, costCalculator, session, stageInfo.getPlan(), Optional.of(stageInfo.getStageStats()), Optional.of(aggregatedStats)));
        }

        return builder.toString();
    }

    private static Map<PlanNodeId, PlanNodeStats> aggregatePlanNodeStats(StageInfo stageInfo)
    {
        Map<PlanNodeId, PlanNodeStats> aggregatedStats = new HashMap<>();
        List<PlanNodeStats> planNodeStats = stageInfo.getTasks().stream()
                .map(TaskInfo::getStats)
                .flatMap(taskStats -> getPlanNodeStats(taskStats).stream())
                .collect(toList());
        for (PlanNodeStats stats : planNodeStats) {
            aggregatedStats.merge(stats.getPlanNodeId(), stats, PlanNodeStats::merge);
        }
        return aggregatedStats;
    }

    private static List<PlanNodeStats> getPlanNodeStats(TaskStats taskStats)
    {
        // Best effort to reconstruct the plan nodes from operators.
        // Because stats are collected separately from query execution,
        // it's possible that some or all of them are missing or out of date.
        // For example, a LIMIT clause can cause a query to finish before stats
        // are collected from the leaf stages.
        Map<PlanNodeId, Long> planNodeInputPositions = new HashMap<>();
        Map<PlanNodeId, Long> planNodeInputBytes = new HashMap<>();
        Map<PlanNodeId, Long> planNodeOutputPositions = new HashMap<>();
        Map<PlanNodeId, Long> planNodeOutputBytes = new HashMap<>();
        Map<PlanNodeId, Long> planNodeWallMillis = new HashMap<>();

        Map<PlanNodeId, Map<String, OperatorInputStats>> operatorInputStats = new HashMap<>();
        Map<PlanNodeId, Map<String, OperatorHashCollisionsStats>> operatorHashCollisionsStats = new HashMap<>();

        Map<PlanNodeId, WindowNodeStats> windowNodeStats = new HashMap<>();

        for (PipelineStats pipelineStats : taskStats.getPipelines()) {
            // Due to eventual consistently collected stats, these could be empty
            if (pipelineStats.getOperatorSummaries().isEmpty()) {
                continue;
            }

            Set<PlanNodeId> processedNodes = new HashSet<>();
            PlanNodeId inputPlanNode = pipelineStats.getOperatorSummaries().iterator().next().getPlanNodeId();
            PlanNodeId outputPlanNode = getLast(pipelineStats.getOperatorSummaries()).getPlanNodeId();

            // Gather input statistics
            for (OperatorStats operatorStats : pipelineStats.getOperatorSummaries()) {
                PlanNodeId planNodeId = operatorStats.getPlanNodeId();

                long wall = operatorStats.getAddInputWall().toMillis() + operatorStats.getGetOutputWall().toMillis() + operatorStats.getFinishWall().toMillis();
                planNodeWallMillis.merge(planNodeId, wall, Long::sum);

                // A pipeline like hash build before join might link to another "internal" pipelines which provide actual input for this plan node
                if (operatorStats.getPlanNodeId().equals(inputPlanNode) && !pipelineStats.isInputPipeline()) {
                    continue;
                }
                if (processedNodes.contains(planNodeId)) {
                    continue;
                }

                operatorInputStats.merge(planNodeId,
                        ImmutableMap.of(
                                operatorStats.getOperatorType(),
                                new OperatorInputStats(
                                        operatorStats.getTotalDrivers(),
                                        operatorStats.getInputPositions(),
                                        operatorStats.getSumSquaredInputPositions())),
                        PlanPrinter::mergeOperatorInputStatsMaps);

                if (operatorStats.getInfo() instanceof HashCollisionsInfo) {
                    HashCollisionsInfo hashCollisionsInfo = (HashCollisionsInfo) operatorStats.getInfo();
                    operatorHashCollisionsStats.merge(planNodeId,
                            ImmutableMap.of(
                                    operatorStats.getOperatorType(),
                                    new OperatorHashCollisionsStats(
                                            hashCollisionsInfo.getWeightedHashCollisions(),
                                            hashCollisionsInfo.getWeightedSumSquaredHashCollisions(),
                                            hashCollisionsInfo.getWeightedExpectedHashCollisions())),
                            PlanPrinter::mergeOperatorHashCollisionsStatsMaps);
                }

                if (operatorStats.getInfo() instanceof WindowInfo) {
                    WindowInfo windowInfo = (WindowInfo) operatorStats.getInfo();
                    windowNodeStats.merge(planNodeId, WindowNodeStats.create(windowInfo), WindowNodeStats::merge);
                }

                planNodeInputPositions.merge(planNodeId, operatorStats.getInputPositions(), Long::sum);
                planNodeInputBytes.merge(planNodeId, operatorStats.getInputDataSize().toBytes(), Long::sum);
                processedNodes.add(planNodeId);
            }

            // Gather output statistics
            processedNodes.clear();
            for (OperatorStats operatorStats : reverse(pipelineStats.getOperatorSummaries())) {
                PlanNodeId planNodeId = operatorStats.getPlanNodeId();

                // An "internal" pipeline like a hash build, links to another pipeline which is the actual output for this plan node
                if (operatorStats.getPlanNodeId().equals(outputPlanNode) && !pipelineStats.isOutputPipeline()) {
                    continue;
                }
                if (processedNodes.contains(planNodeId)) {
                    continue;
                }

                planNodeOutputPositions.merge(planNodeId, operatorStats.getOutputPositions(), Long::sum);
                planNodeOutputBytes.merge(planNodeId, operatorStats.getOutputDataSize().toBytes(), Long::sum);
                processedNodes.add(planNodeId);
            }
        }

        List<PlanNodeStats> stats = new ArrayList<>();
        for (Map.Entry<PlanNodeId, Long> entry : planNodeWallMillis.entrySet()) {
            PlanNodeId planNodeId = entry.getKey();
            stats.add(new PlanNodeStats(
                    planNodeId,
                    new Duration(planNodeWallMillis.get(planNodeId), MILLISECONDS),
                    planNodeInputPositions.get(planNodeId),
                    succinctDataSize(planNodeInputBytes.get(planNodeId), BYTE),
                    // It's possible there will be no output stats because all the pipelines that we observed were non-output.
                    // For example in a query like SELECT * FROM a JOIN b ON c = d LIMIT 1
                    // It's possible to observe stats after the build starts, but before the probe does
                    // and therefore only have wall time, but no output stats
                    planNodeOutputPositions.getOrDefault(planNodeId, 0L),
                    succinctDataSize(planNodeOutputBytes.getOrDefault(planNodeId, 0L), BYTE),
                    operatorInputStats.get(planNodeId),
                    // Only some operators emit hash collisions statistics
                    operatorHashCollisionsStats.getOrDefault(planNodeId, emptyMap()),
                    Optional.ofNullable(windowNodeStats.get(planNodeId))));
        }
        return stats;
    }

    public static String textDistributedPlan(SubPlan plan, Metadata metadata, CostCalculator costCalculator, Session session)
    {
        StringBuilder builder = new StringBuilder();
        for (PlanFragment fragment : plan.getAllFragments()) {
            builder.append(formatFragment(metadata, costCalculator, session, fragment, Optional.empty(), Optional.empty()));
        }

        return builder.toString();
    }

    private static String formatFragment(Metadata metadata, CostCalculator costCalculator, Session session, PlanFragment fragment, Optional<StageStats> stageStats, Optional<Map<PlanNodeId, PlanNodeStats>> planNodeStats)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(format("Fragment %s [%s]\n",
                fragment.getId(),
                fragment.getPartitioning()));

        if (stageStats.isPresent()) {
            builder.append(indentString(1))
                    .append(format("Cost: CPU %s, Input: %s (%s), Output: %s (%s)\n",
                            stageStats.get().getTotalCpuTime(),
                            formatPositions(stageStats.get().getProcessedInputPositions()),
                            stageStats.get().getProcessedInputDataSize(),
                            formatPositions(stageStats.get().getOutputPositions()),
                            stageStats.get().getOutputDataSize()));
        }

        PartitioningScheme partitioningScheme = fragment.getPartitioningScheme();
        builder.append(indentString(1))
                .append(format("Output layout: [%s]\n",
                        Joiner.on(", ").join(partitioningScheme.getOutputLayout())));

        boolean replicateNulls = partitioningScheme.isReplicateNulls();
        List<String> arguments = partitioningScheme.getPartitioning().getArguments().stream()
                .map(argument -> {
                    if (argument.isConstant()) {
                        NullableValue constant = argument.getConstant();
                        String printableValue = castToVarchar(constant.getType(), constant.getValue(), metadata, session);
                        return constant.getType().getDisplayName() + "(" + printableValue + ")";
                    }
                    return argument.getColumn().toString();
                })
                .collect(toImmutableList());
        builder.append(indentString(1));
        if (replicateNulls) {
            builder.append(format("Output partitioning: %s (replicate nulls) [%s]%s\n",
                    partitioningScheme.getPartitioning().getHandle(),
                    Joiner.on(", ").join(arguments),
                    formatHash(partitioningScheme.getHashColumn())));
        }
        else {
            builder.append(format("Output partitioning: %s [%s]%s\n",
                    partitioningScheme.getPartitioning().getHandle(),
                    Joiner.on(", ").join(arguments),
                    formatHash(partitioningScheme.getHashColumn())));
        }

        if (stageStats.isPresent()) {
            builder.append(textLogicalPlan(fragment.getRoot(), fragment.getSymbols(), metadata, costCalculator, session, planNodeStats.get(), 1))
                    .append("\n");
        }
        else {
            builder.append(textLogicalPlan(fragment.getRoot(), fragment.getSymbols(), metadata, costCalculator, session, 1))
                    .append("\n");
        }

        return builder.toString();
    }

    public static String graphvizLogicalPlan(PlanNode plan, Map<Symbol, Type> types)
    {
        PlanFragment fragment = new PlanFragment(
                new PlanFragmentId("graphviz_plan"),
                plan,
                types,
                SINGLE_DISTRIBUTION,
                ImmutableList.of(plan.getId()),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), plan.getOutputSymbols()));
        return GraphvizPrinter.printLogical(ImmutableList.of(fragment));
    }

    public static String graphvizDistributedPlan(SubPlan plan)
    {
        return GraphvizPrinter.printDistributed(plan);
    }

    private void print(int indent, String format, Object... args)
    {
        String value;

        if (args.length == 0) {
            value = format;
        }
        else {
            value = format(format, args);
        }
        output.append(indentString(indent)).append(value).append('\n');
    }

    private void print(int indent, String format, List<Object> args)
    {
        print(indent, format, args.toArray(new Object[args.size()]));
    }

    private void printStats(int intent, PlanNodeId planNodeId)
    {
        printStats(intent, planNodeId, false, false);
    }

    private void printStats(int indent, PlanNodeId planNodeId, boolean printInput, boolean printFiltered)
    {
        if (!stats.isPresent()) {
            return;
        }

        long totalMillis = stats.get().values().stream()
                .mapToLong(node -> node.getPlanNodeWallTime().toMillis())
                .sum();

        PlanNodeStats nodeStats = stats.get().get(planNodeId);
        if (nodeStats == null) {
            output.append(indentString(indent));
            output.append("Cost: ?");
            if (printInput) {
                output.append(", Input: ? lines (?B)");
            }
            output.append(", Output: ? lines (?B)");
            if (printFiltered) {
                output.append(", Filtered: ?%");
            }
            output.append('\n');
            return;
        }

        double fraction = 100.0d * nodeStats.getPlanNodeWallTime().toMillis() / totalMillis;

        output.append(indentString(indent));
        output.append("Cost: " + formatDouble(fraction) + "%");
        if (printInput) {
            output.append(format(", Input: %s (%s)",
                    formatPositions(nodeStats.getPlanNodeInputPositions()),
                    nodeStats.getPlanNodeInputDataSize().toString()));
        }
        output.append(format(", Output: %s (%s)",
                formatPositions(nodeStats.getPlanNodeOutputPositions()),
                nodeStats.getPlanNodeOutputDataSize().toString()));
        if (printFiltered) {
            double filtered = 100.0d * (nodeStats.getPlanNodeInputPositions() - nodeStats.getPlanNodeOutputPositions()) / nodeStats.getPlanNodeInputPositions();
            output.append(", Filtered: " + formatDouble(filtered) + "%");
        }
        output.append('\n');

        printDistributions(indent, nodeStats);

        if (nodeStats.getWindowNodeStats().isPresent()) {
            printWindowNodeStats(indent, nodeStats.getWindowNodeStats().get());
        }
    }

    private void printDistributions(int indent, PlanNodeStats nodeStats)
    {
        Map<String, Double> inputAverages = nodeStats.getOperatorInputPositionsAverages();
        Map<String, Double> inputStdDevs = nodeStats.getOperatorInputPositionsStdDevs();

        Map<String, Double> hashCollisionsAverages = nodeStats.getOperatorHashCollisionsAverages();
        Map<String, Double> hashCollisionsStdDevs = nodeStats.getOperatorHashCollisionsStdDevs();
        Map<String, Double> expectedHashCollisionsAverages = nodeStats.getOperatorExpectedCollisionsAverages();

        Map<String, String> translatedOperatorTypes = translateOperatorTypes(nodeStats.getOperatorTypes());

        for (String operator : translatedOperatorTypes.keySet()) {
            String translatedOperatorType = translatedOperatorTypes.get(operator);
            double inputAverage = inputAverages.get(operator);

            output.append(indentString(indent));
            output.append(translatedOperatorType);
            output.append(format(Locale.US, "Input avg.: %s lines, Input std.dev.: %s%%",
                    formatDouble(inputAverage), formatDouble(100.0d * inputStdDevs.get(operator) / inputAverage)));
            output.append('\n');

            double hashCollisionsAverage = hashCollisionsAverages.getOrDefault(operator, 0.0d);
            double expectedHashCollisionsAverage = expectedHashCollisionsAverages.getOrDefault(operator, 0.0d);
            if (hashCollisionsAverage != 0.0d) {
                double hashCollisionsStdDevRatio = hashCollisionsStdDevs.get(operator) / hashCollisionsAverage;

                if (translatedOperatorType.isEmpty()) {
                    output.append(indentString(indent));
                }
                else {
                    output.append(indentString(indent + 2));
                }

                if (expectedHashCollisionsAverage != 0.0d) {
                    double hashCollisionsRatio = hashCollisionsAverage / expectedHashCollisionsAverage;
                    output.append(format(Locale.US, "Collisions avg.: %s (%s%% est.), Collisions std.dev.: %s%%",
                            formatDouble(hashCollisionsAverage), formatDouble(hashCollisionsRatio * 100.0d), formatDouble(hashCollisionsStdDevRatio * 100.0d)));
                }
                else {
                    output.append(format(Locale.US, "Collisions avg.: %s, Collisions std.dev.: %s%%",
                            formatDouble(hashCollisionsAverage), formatDouble(hashCollisionsStdDevRatio * 100.0d)));
                }

                output.append('\n');
            }
        }
    }

    private static Map<String, String> translateOperatorTypes(Set<String> operators)
    {
        if (operators.size() == 1) {
            // don't display operator (plan node) name again
            return ImmutableMap.of(getOnlyElement(operators), "");
        }

        if (operators.contains("LookupJoinOperator") && operators.contains("HashBuilderOperator")) {
            // join plan node
            return ImmutableMap.of(
                    "LookupJoinOperator", "Left (probe) ",
                    "HashBuilderOperator", "Right (build) ");
        }

        return ImmutableMap.of();
    }

    private void printWindowNodeStats(int indent, WindowNodeStats stats)
    {
        output.append(indentString(indent));
        output.append(format("Active Drivers: [ %d / %d ]", stats.getActiveDrivers(), stats.getTotalDrivers()));
        output.append('\n');

        output.append(indentString(indent));
        output.append(formatDistribution("Index size in bytes", stats.getIndexSizeDistribution()));
        output.append('\n');

        output.append(indentString(indent));
        output.append(formatDistribution("Rows in partitions per index", stats.getPartitionRowsPerIndexDistribution()));
        output.append('\n');

        output.append(indentString(indent));
        output.append(formatDistribution("Rows in partitions per driver", stats.getPartitionRowsPerDriverDistribution()));
        output.append('\n');
    }

    private static String formatDouble(double value)
    {
        if (isFinite(value)) {
            return format(Locale.US, "%.2f", value);
        }

        return "?";
    }

    private static String formatPositions(long positions)
    {
        if (positions == 1) {
            return "1 row";
        }

        return positions + " rows";
    }

    private static String formatDistribution(String name, DistributionSnapshot d)
    {
        return format(Locale.US,
                "%s: { count: %.0f, total: %.0f, min: %d, p05: %d, p25: %d, p50: %d, p75: %d, p95: %d, max: %d }",
                name, d.getCount(), d.getTotal(), d.getMin(), d.getP05(), d.getP25(), d.getP50(), d.getP75(), d.getP95(), d.getMax());
    }

    private static String indentString(int indent)
    {
        return Strings.repeat("    ", indent);
    }

    private class Visitor
            extends PlanVisitor<Integer, Void>
    {
        private final Map<Symbol, Type> types;
        private final Map<PlanNode, PlanNodeCost> costs;
        private final Session session;

        @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
        public Visitor(Map<Symbol, Type> types, Map<PlanNode, PlanNodeCost> costs, Session session)
        {
            this.types = types;
            this.costs = costs;
            this.session = session;
        }

        @Override
        public Void visitExplainAnalyze(ExplainAnalyzeNode node, Integer indent)
        {
            print(indent, "- ExplainAnalyze => [%s] %s", formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitJoin(JoinNode node, Integer indent)
        {
            List<Expression> joinExpressions = new ArrayList<>();
            for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(new ComparisonExpression(ComparisonExpressionType.EQUAL,
                        clause.getLeft().toSymbolReference(),
                        clause.getRight().toSymbolReference()));
            }
            node.getFilter().ifPresent(expression -> joinExpressions.add(expression));

            // Check if the node is actually a cross join node
            if (node.getType() == JoinNode.Type.INNER && joinExpressions.isEmpty()) {
                print(indent, "- CrossJoin => [%s] %s", formatOutputs(node.getOutputSymbols()), formatCost(node));
            }
            else {
                print(indent, "- %s[%s]%s => [%s] %s",
                        node.getType().getJoinLabel(),
                        Joiner.on(" AND ").join(joinExpressions),
                        formatHash(node.getLeftHashSymbol(), node.getRightHashSymbol()),
                        formatOutputs(node.getOutputSymbols()),
                        formatCost(node));
            }

            printStats(indent + 2, node.getId());
            node.getLeft().accept(this, indent + 1);
            node.getRight().accept(this, indent + 1);

            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Integer indent)
        {
            print(indent, "- SemiJoin[%s = %s]%s => [%s] %s",
                    node.getSourceJoinSymbol(),
                    node.getFilteringSourceJoinSymbol(),
                    formatHash(node.getSourceHashSymbol(), node.getFilteringSourceHashSymbol()),
                    formatOutputs(node.getOutputSymbols()),
                    formatCost(node));
            printStats(indent + 2, node.getId());
            node.getSource().accept(this, indent + 1);
            node.getFilteringSource().accept(this, indent + 1);

            return null;
        }

        @Override
        public Void visitIndexSource(IndexSourceNode node, Integer indent)
        {
            print(indent, "- IndexSource[%s, lookup = %s] => [%s] %s", node.getIndexHandle(), node.getLookupSymbols(), formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());
            for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                if (node.getOutputSymbols().contains(entry.getKey())) {
                    print(indent + 2, "%s := %s", entry.getKey(), entry.getValue());
                }
            }
            return null;
        }

        @Override
        public Void visitIndexJoin(IndexJoinNode node, Integer indent)
        {
            List<Expression> joinExpressions = new ArrayList<>();
            for (IndexJoinNode.EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(new ComparisonExpression(ComparisonExpressionType.EQUAL,
                        clause.getProbe().toSymbolReference(),
                        clause.getIndex().toSymbolReference()));
            }

            print(indent, "- %sIndexJoin[%s]%s => [%s] %s",
                    node.getType().getJoinLabel(),
                    Joiner.on(" AND ").join(joinExpressions),
                    formatHash(node.getProbeHashSymbol(), node.getIndexHashSymbol()),
                    formatOutputs(node.getOutputSymbols()),
                    formatCost(node));
            printStats(indent + 2, node.getId());
            node.getProbeSource().accept(this, indent + 1);
            node.getIndexSource().accept(this, indent + 1);

            return null;
        }

        @Override
        public Void visitLimit(LimitNode node, Integer indent)
        {
            print(indent, "- Limit%s[%s] => [%s] %s", node.isPartial() ? "Partial" : "", node.getCount(), formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitDistinctLimit(DistinctLimitNode node, Integer indent)
        {
            print(indent, "- DistinctLimit%s[%s]%s => [%s] %s",
                    node.isPartial() ? "Partial" : "",
                    node.getLimit(),
                    formatHash(node.getHashSymbol()),
                    formatOutputs(node.getOutputSymbols()),
                    formatCost(node));
            printStats(indent + 2, node.getId());
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitAggregation(AggregationNode node, Integer indent)
        {
            String type = "";
            if (node.getStep() != AggregationNode.Step.SINGLE) {
                type = format("(%s)", node.getStep().toString());
            }
            String key = "";
            if (!node.getGroupingKeys().isEmpty()) {
                key = node.getGroupingKeys().toString();
            }

            print(indent, "- Aggregate%s%s%s => [%s] %s", type, key, formatHash(node.getHashSymbol()), formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());

            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                if (node.getMasks().containsKey(entry.getKey())) {
                    print(indent + 2, "%s := %s (mask = %s)", entry.getKey(), entry.getValue(), node.getMasks().get(entry.getKey()));
                }
                else {
                    print(indent + 2, "%s := %s", entry.getKey(), entry.getValue());
                }
            }

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitGroupId(GroupIdNode node, Integer indent)
        {
            // grouping sets are easier to understand in terms of inputs
            List<List<Symbol>> inputGroupingSetSymbols = node.getGroupingSets().stream()
                    .map(set -> set.stream()
                            .map(symbol -> node.getGroupingSetMappings().get(symbol))
                            .collect(Collectors.toList()))
                    .collect(Collectors.toList());

            print(indent, "- GroupId%s => [%s] %s", inputGroupingSetSymbols, formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());

            for (Map.Entry<Symbol, Symbol> mapping : node.getGroupingSetMappings().entrySet()) {
                print(indent + 2, "%s := %s", mapping.getKey(), mapping.getValue());
            }
            for (Map.Entry<Symbol, Symbol> argument : node.getArgumentMappings().entrySet()) {
                print(indent + 2, "%s := %s", argument.getKey(), argument.getValue());
            }

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitMarkDistinct(MarkDistinctNode node, Integer indent)
        {
            print(indent, "- MarkDistinct[distinct=%s marker=%s]%s => [%s] %s",
                    formatOutputs(node.getDistinctSymbols()),
                    node.getMarkerSymbol(),
                    formatHash(node.getHashSymbol()),
                    formatOutputs(node.getOutputSymbols()),
                    formatCost(node));

            printStats(indent + 2, node.getId());
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitWindow(WindowNode node, Integer indent)
        {
            List<String> partitionBy = Lists.transform(node.getPartitionBy(), Functions.toStringFunction());

            List<String> orderBy = Lists.transform(node.getOrderBy(), input -> input + " " + node.getOrderings().get(input));

            List<String> args = new ArrayList<>();
            if (!partitionBy.isEmpty()) {
                List<Symbol> prePartitioned = node.getPartitionBy().stream()
                        .filter(node.getPrePartitionedInputs()::contains)
                        .collect(toImmutableList());

                List<Symbol> notPrePartitioned = node.getPartitionBy().stream()
                        .filter(column -> !node.getPrePartitionedInputs().contains(column))
                        .collect(toImmutableList());

                StringBuilder builder = new StringBuilder();
                if (!prePartitioned.isEmpty()) {
                    builder.append("<")
                            .append(Joiner.on(", ").join(prePartitioned))
                            .append(">");
                    if (!notPrePartitioned.isEmpty()) {
                        builder.append(", ");
                    }
                }
                if (!notPrePartitioned.isEmpty()) {
                    builder.append(Joiner.on(", ").join(notPrePartitioned));
                }
                args.add(format("partition by (%s)", builder));
            }
            if (!orderBy.isEmpty()) {
                args.add(format("order by (%s)", Stream.concat(
                        node.getOrderBy().stream()
                                .limit(node.getPreSortedOrderPrefix())
                                .map(symbol -> "<" + symbol + " " + node.getOrderings().get(symbol) + ">"),
                        node.getOrderBy().stream()
                                .skip(node.getPreSortedOrderPrefix())
                                .map(symbol -> symbol + " " + node.getOrderings().get(symbol)))
                        .collect(Collectors.joining(", "))));
            }

            print(indent, "- Window[%s]%s => [%s] %s", Joiner.on(", ").join(args), formatHash(node.getHashSymbol()), formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());

            for (Map.Entry<Symbol, WindowNode.Function> entry : node.getWindowFunctions().entrySet()) {
                FunctionCall call = entry.getValue().getFunctionCall();
                String frameInfo = call.getWindow()
                        .flatMap(Window::getFrame)
                        .map(PlanPrinter::formatFrame)
                        .orElse("");

                print(indent + 2, "%s := %s(%s) %s", entry.getKey(), call.getName(), Joiner.on(", ").join(call.getArguments()), frameInfo);
            }
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitTopNRowNumber(TopNRowNumberNode node, Integer indent)
        {
            List<String> partitionBy = Lists.transform(node.getPartitionBy(), Functions.toStringFunction());

            List<String> orderBy = Lists.transform(node.getOrderBy(), input -> input + " " + node.getOrderings().get(input));

            List<String> args = new ArrayList<>();
            args.add(format("partition by (%s)", Joiner.on(", ").join(partitionBy)));
            args.add(format("order by (%s)", Joiner.on(", ").join(orderBy)));

            print(indent, "- TopNRowNumber[%s limit %s]%s => [%s] %s",
                    Joiner.on(", ").join(args),
                    node.getMaxRowCountPerPartition(),
                    formatHash(node.getHashSymbol()),
                    formatOutputs(node.getOutputSymbols()),
                    formatCost(node));
            printStats(indent + 2, node.getId());

            print(indent + 2, "%s := %s", node.getRowNumberSymbol(), "row_number()");
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitRowNumber(RowNumberNode node, Integer indent)
        {
            List<String> partitionBy = Lists.transform(node.getPartitionBy(), Functions.toStringFunction());
            List<String> args = new ArrayList<>();
            if (!partitionBy.isEmpty()) {
                args.add(format("partition by (%s)", Joiner.on(", ").join(partitionBy)));
            }

            if (node.getMaxRowCountPerPartition().isPresent()) {
                args.add(format("limit = %s", node.getMaxRowCountPerPartition().get()));
            }

            print(indent, "- RowNumber[%s]%s => [%s] %s",
                    Joiner.on(", ").join(args),
                    formatHash(node.getHashSymbol()),
                    formatOutputs(node.getOutputSymbols()),
                    formatCost(node));
            printStats(indent + 2, node.getId());

            print(indent + 2, "%s := %s", node.getRowNumberSymbol(), "row_number()");
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Integer indent)
        {
            TableHandle table = node.getTable();
            print(indent, "- TableScan[%s, originalConstraint = %s] => [%s] %s", table, node.getOriginalConstraint(), formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());
            printTableScanInfo(node, indent);

            return null;
        }

        @Override
        public Void visitValues(ValuesNode node, Integer indent)
        {
            print(indent, "- Values => [%s] %s", formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());
            for (List<Expression> row : node.getRows()) {
                print(indent + 2, "(" + Joiner.on(", ").join(row) + ")");
            }
            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, Integer indent)
        {
            return visitScanFilterAndProjectInfo(node.getId(), Optional.of(node), Optional.empty(), indent);
        }

        @Override
        public Void visitProject(ProjectNode node, Integer indent)
        {
            if (node.getSource() instanceof FilterNode) {
                return visitScanFilterAndProjectInfo(node.getId(), Optional.of((FilterNode) node.getSource()), Optional.of(node), indent);
            }

            return visitScanFilterAndProjectInfo(node.getId(), Optional.empty(), Optional.of(node), indent);
        }

        private Void visitScanFilterAndProjectInfo(
                PlanNodeId planNodeId,
                Optional<FilterNode> filterNode, Optional<ProjectNode> projectNode,
                int indent)
        {
            checkState(projectNode.isPresent() || filterNode.isPresent());

            PlanNode sourceNode;
            if (filterNode.isPresent()) {
                sourceNode = filterNode.get().getSource();
            }
            else {
                sourceNode = projectNode.get().getSource();
            }

            Optional<TableScanNode> scanNode;
            if (sourceNode instanceof TableScanNode) {
                scanNode = Optional.of((TableScanNode) sourceNode);
            }
            else {
                scanNode = Optional.empty();
            }

            String format = "[";
            String operatorName = "- ";
            List<Object> arguments = new LinkedList<>();

            if (scanNode.isPresent()) {
                operatorName += "Scan";
                format += "table = %s, originalConstraint = %s";
                if (filterNode.isPresent()) {
                    format += ", ";
                }
                TableHandle table = scanNode.get().getTable();
                arguments.add(table);
                arguments.add(scanNode.get().getOriginalConstraint());
            }

            if (filterNode.isPresent()) {
                operatorName += "Filter";
                format += "filterPredicate = %s";
                arguments.add(filterNode.get().getPredicate());
            }

            format += "] => [%s]";
            if (projectNode.isPresent()) {
                operatorName += "Project";
                arguments.add(formatOutputs(projectNode.get().getOutputSymbols()));
            }
            else {
                arguments.add(formatOutputs(filterNode.get().getOutputSymbols()));
            }

            format += " %s";
            arguments.add(
                    Joiner.on("/").join(ImmutableList.of(scanNode, filterNode, projectNode).stream()
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .map(this::formatCost)
                            .collect(toImmutableList())));

            format = operatorName + format;
            print(indent, format, arguments);
            printStats(indent + 2, planNodeId, true, true);

            if (projectNode.isPresent()) {
                printAssignments(projectNode.get().getAssignments(), indent + 2);
            }

            if (scanNode.isPresent()) {
                printTableScanInfo(scanNode.get(), indent);
                return null;
            }

            sourceNode.accept(this, indent + 1);
            return null;
        }

        private void printTableScanInfo(TableScanNode node, int indent)
        {
            TableHandle table = node.getTable();

            TupleDomain<ColumnHandle> predicate = node.getLayout()
                    .map(layoutHandle -> metadata.getLayout(session, layoutHandle))
                    .map(TableLayout::getPredicate)
                    .orElse(TupleDomain.all());

            if (node.getLayout().isPresent()) {
                // TODO: find a better way to do this
                ConnectorTableLayoutHandle layout = node.getLayout().get().getConnectorHandle();
                if (!table.getConnectorHandle().toString().equals(layout.toString())) {
                    print(indent + 2, "LAYOUT: %s", layout);
                }
            }

            if (predicate.isNone()) {
                print(indent + 2, ":: NONE");
            }
            else {
                // first, print output columns and their constraints
                for (Map.Entry<Symbol, ColumnHandle> assignment : node.getAssignments().entrySet()) {
                    ColumnHandle column = assignment.getValue();
                    print(indent + 2, "%s := %s", assignment.getKey(), column);
                    printConstraint(indent + 3, column, predicate);
                }

                // then, print constraints for columns that are not in the output
                if (!predicate.isAll()) {
                    Set<ColumnHandle> outputs = ImmutableSet.copyOf(node.getAssignments().values());

                    predicate.getDomains().get()
                            .entrySet().stream()
                            .filter(entry -> !outputs.contains(entry.getKey()))
                            .forEach(entry -> {
                                ColumnHandle column = entry.getKey();
                                print(indent + 2, "%s", column);
                                printConstraint(indent + 3, column, predicate);
                            });
                }
            }
        }

        @Override
        public Void visitUnnest(UnnestNode node, Integer indent)
        {
            print(indent, "- Unnest [replicate=%s, unnest=%s] => [%s] %s", formatOutputs(node.getReplicateSymbols()), formatOutputs(node.getUnnestSymbols().keySet()), formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitOutput(OutputNode node, Integer indent)
        {
            print(indent, "- Output[%s] => [%s] %s", Joiner.on(", ").join(node.getColumnNames()), formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());
            for (int i = 0; i < node.getColumnNames().size(); i++) {
                String name = node.getColumnNames().get(i);
                Symbol symbol = node.getOutputSymbols().get(i);
                if (!name.equals(symbol.toString())) {
                    print(indent + 2, "%s := %s", name, symbol);
                }
            }

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitTopN(TopNNode node, Integer indent)
        {
            Iterable<String> keys = Iterables.transform(node.getOrderBy(), input -> input + " " + node.getOrderings().get(input));

            print(indent, "- TopN[%s by (%s)] => [%s] %s", node.getCount(), Joiner.on(", ").join(keys), formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitSort(SortNode node, Integer indent)
        {
            Iterable<String> keys = Iterables.transform(node.getOrderBy(), input -> input + " " + node.getOrderings().get(input));

            print(indent, "- Sort[%s] => [%s] %s", Joiner.on(", ").join(keys), formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitRemoteSource(RemoteSourceNode node, Integer indent)
        {
            print(indent, "- RemoteSource[%s] => [%s] %s", Joiner.on(',').join(node.getSourceFragmentIds()), formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());

            return null;
        }

        @Override
        public Void visitUnion(UnionNode node, Integer indent)
        {
            print(indent, "- Union => [%s] %s", formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitIntersect(IntersectNode node, Integer indent)
        {
            print(indent, "- Intersect => [%s] %s", formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitExcept(ExceptNode node, Integer indent)
        {
            print(indent, "- Except => [%s] %s", formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitTableWriter(TableWriterNode node, Integer indent)
        {
            print(indent, "- TableWriter => [%s] %s", formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());
            for (int i = 0; i < node.getColumnNames().size(); i++) {
                String name = node.getColumnNames().get(i);
                Symbol symbol = node.getColumns().get(i);
                print(indent + 2, "%s := %s", name, symbol);
            }

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitTableFinish(TableFinishNode node, Integer indent)
        {
            print(indent, "- TableCommit[%s] => [%s] %s", node.getTarget(), formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitSample(SampleNode node, Integer indent)
        {
            print(indent, "- Sample[%s: %s] => [%s] %s", node.getSampleType(), node.getSampleRatio(), formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitExchange(ExchangeNode node, Integer indent)
        {
            if (node.getScope() == Scope.LOCAL) {
                print(indent, "- LocalExchange[%s%s]%s (%s) => %s %s",
                        node.getPartitioningScheme().getPartitioning().getHandle(),
                        node.getPartitioningScheme().isReplicateNulls() ? " - REPLICATE NULLS" : "",
                        formatHash(node.getPartitioningScheme().getHashColumn()),
                        Joiner.on(", ").join(node.getPartitioningScheme().getPartitioning().getArguments()),
                        formatOutputs(node.getOutputSymbols()),
                        formatCost(node));
            }
            else {
                print(indent, "- %sExchange[%s%s]%s => %s %s",
                        UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, node.getScope().toString()),
                        node.getType(),
                        node.getPartitioningScheme().isReplicateNulls() ? " - REPLICATE NULLS" : "",
                        formatHash(node.getPartitioningScheme().getHashColumn()),
                        formatOutputs(node.getOutputSymbols()),
                        formatCost(node));
            }
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitDelete(DeleteNode node, Integer indent)
        {
            print(indent, "- Delete[%s] => [%s] %s", node.getTarget(), formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitMetadataDelete(MetadataDeleteNode node, Integer indent)
        {
            print(indent, "- MetadataDelete[%s] => [%s] %s", node.getTarget(), formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitEnforceSingleRow(EnforceSingleRowNode node, Integer indent)
        {
            print(indent, "- Scalar => [%s] %s", formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitAssignUniqueId(AssignUniqueId node, Integer indent)
        {
            print(indent, "- AssignUniqueId => [%s] %s", formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitApply(ApplyNode node, Integer indent)
        {
            print(indent, "- Apply[%s] => [%s] %s", node.getCorrelation(), formatOutputs(node.getOutputSymbols()), formatCost(node));
            printStats(indent + 2, node.getId());
            printAssignments(node.getSubqueryAssignments(), indent + 4);

            return processChildren(node, indent + 1);
        }

        @Override
        protected Void visitPlan(PlanNode node, Integer indent)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        private Void processChildren(PlanNode node, int indent)
        {
            for (PlanNode child : node.getSources()) {
                child.accept(this, indent);
            }

            return null;
        }

        private void printAssignments(Assignments assignments, int indent)
        {
            for (Map.Entry<Symbol, Expression> entry : assignments.getMap().entrySet()) {
                if (entry.getValue() instanceof SymbolReference && ((SymbolReference) entry.getValue()).getName().equals(entry.getKey().getName())) {
                    // skip identity assignments
                    continue;
                }
                print(indent, "%s := %s", entry.getKey(), entry.getValue());
            }
        }

        private String formatOutputs(Iterable<Symbol> symbols)
        {
            return Joiner.on(", ").join(Iterables.transform(symbols, input -> input + ":" + types.get(input).getDisplayName()));
        }

        private void printConstraint(int indent, ColumnHandle column, TupleDomain<ColumnHandle> constraint)
        {
            checkArgument(!constraint.isNone());
            Map<ColumnHandle, Domain> domains = constraint.getDomains().get();
            if (!constraint.isAll() && domains.containsKey(column)) {
                print(indent, ":: %s", formatDomain(simplifyDomain(domains.get(column))));
            }
        }

        private String formatDomain(Domain domain)
        {
            ImmutableList.Builder<String> parts = ImmutableList.builder();

            if (domain.isNullAllowed()) {
                parts.add("NULL");
            }

            Type type = domain.getType();

            domain.getValues().getValuesProcessor().consume(
                    ranges -> {
                        for (Range range : ranges.getOrderedRanges()) {
                            StringBuilder builder = new StringBuilder();
                            if (range.isSingleValue()) {
                                String value = castToVarchar(type, range.getSingleValue(), PlanPrinter.this.metadata, session);
                                builder.append('[').append(value).append(']');
                            }
                            else {
                                builder.append((range.getLow().getBound() == Marker.Bound.EXACTLY) ? '[' : '(');

                                if (range.getLow().isLowerUnbounded()) {
                                    builder.append("<min>");
                                }
                                else {
                                    builder.append(castToVarchar(type, range.getLow().getValue(), PlanPrinter.this.metadata, session));
                                }

                                builder.append(", ");

                                if (range.getHigh().isUpperUnbounded()) {
                                    builder.append("<max>");
                                }
                                else {
                                    builder.append(castToVarchar(type, range.getHigh().getValue(), PlanPrinter.this.metadata, session));
                                }

                                builder.append((range.getHigh().getBound() == Marker.Bound.EXACTLY) ? ']' : ')');
                            }
                            parts.add(builder.toString());
                        }
                    },
                    discreteValues -> discreteValues.getValues().stream()
                            .map(value -> castToVarchar(type, value, PlanPrinter.this.metadata, session))
                            .sorted() // Sort so the values will be printed in predictable order
                            .forEach(parts::add),
                    allOrNone -> {
                        if (allOrNone.isAll()) {
                            parts.add("ALL VALUES");
                        }
                    });

            return "[" + Joiner.on(", ").join(parts.build()) + "]";
        }

        private String formatCost(PlanNode node)
        {
            PlanNodeCost cost = costs.getOrDefault(node, UNKNOWN_COST);
            Estimate outputRowCount = cost.getOutputRowCount();
            Estimate outputSizeInBytes = cost.getOutputSizeInBytes();
            return String.format("{rows: %s, bytes: %s}",
                    outputRowCount.isValueUnknown() ? "?" : String.valueOf((long) outputRowCount.getValue()),
                    outputSizeInBytes.isValueUnknown() ? "?" : succinctBytes((long) outputSizeInBytes.getValue()));
        }
    }

    private static String formatHash(Optional<Symbol>... hashes)
    {
        List<Symbol> symbols = Arrays.stream(hashes)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());

        if (symbols.isEmpty()) {
            return "";
        }

        return "[" + Joiner.on(", ").join(symbols) + "]";
    }

    private static String formatFrame(WindowFrame frame)
    {
        StringBuilder builder = new StringBuilder(frame.getType().toString());
        FrameBound start = frame.getStart();
        if (start.getValue().isPresent()) {
            builder.append(" ").append(start.getOriginalValue().get());
        }
        builder.append(" ").append(start.getType());

        Optional<FrameBound> end = frame.getEnd();
        if (end.isPresent()) {
            if (end.get().getOriginalValue().isPresent()) {
                builder.append(" ").append(end.get().getOriginalValue().get());
            }
            builder.append(" ").append(end.get().getType());
        }
        return builder.toString();
    }

    private static String castToVarchar(Type type, Object value, Metadata metadata, Session session)
    {
        if (value == null) {
            return "NULL";
        }

        Signature coercion = metadata.getFunctionRegistry().getCoercion(type, VARCHAR);

        try {
            Slice coerced = (Slice) new FunctionInvoker(metadata.getFunctionRegistry()).invoke(coercion, session.toConnectorSession(), value);
            return coerced.toStringUtf8();
        }
        catch (OperatorNotFoundException e) {
            return "<UNREPRESENTABLE VALUE>";
        }
        catch (Throwable throwable) {
            throw Throwables.propagate(throwable);
        }
    }

    private static class PlanNodeStats
    {
        private final PlanNodeId planNodeId;

        private final Duration planNodeWallTime;
        private final long planNodeInputPositions;
        private final DataSize planNodeInputDataSize;
        private final long planNodeOutputPositions;
        private final DataSize planNodeOutputDataSize;

        private final Map<String, OperatorInputStats> operatorInputStats;
        private final Map<String, OperatorHashCollisionsStats> operatorHashCollisionsStats;
        private final Optional<WindowNodeStats> windowNodeStats;

        private PlanNodeStats(
                PlanNodeId planNodeId,
                Duration planNodeWallTime,
                long planNodeInputPositions,
                DataSize planNodeInputDataSize,
                long planNodeOutputPositions,
                DataSize planNodeOutputDataSize,
                Map<String, OperatorInputStats> operatorInputStats,
                Map<String, OperatorHashCollisionsStats> operatorHashCollisionsStats,
                Optional<WindowNodeStats> windowNodeStats)
        {
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");

            this.planNodeWallTime = requireNonNull(planNodeWallTime, "planNodeWallTime is null");
            this.planNodeInputPositions = planNodeInputPositions;
            this.planNodeInputDataSize = planNodeInputDataSize;
            this.planNodeOutputPositions = planNodeOutputPositions;
            this.planNodeOutputDataSize = planNodeOutputDataSize;

            this.operatorInputStats = requireNonNull(operatorInputStats, "operatorInputStats is null");
            this.operatorHashCollisionsStats = requireNonNull(operatorHashCollisionsStats, "operatorHashCollisionsStats is null");
            this.windowNodeStats = requireNonNull(windowNodeStats, "windowNodeStats is null");
        }

        private static double computedStdDev(double sumSquared, double sum, long n)
        {
            double average = sum / n;
            double variance = (sumSquared - 2 * sum * average + average * average * n) / n;
            // variance might be negative because of numeric inaccuracy, therefore we need to use max
            return sqrt(max(variance, 0d));
        }

        public PlanNodeId getPlanNodeId()
        {
            return planNodeId;
        }

        public Duration getPlanNodeWallTime()
        {
            return planNodeWallTime;
        }

        public Set<String> getOperatorTypes()
        {
            return operatorInputStats.keySet();
        }

        public long getPlanNodeInputPositions()
        {
            return planNodeInputPositions;
        }

        public DataSize getPlanNodeInputDataSize()
        {
            return planNodeInputDataSize;
        }

        public long getPlanNodeOutputPositions()
        {
            return planNodeOutputPositions;
        }

        public DataSize getPlanNodeOutputDataSize()
        {
            return planNodeOutputDataSize;
        }

        public Map<String, Double> getOperatorInputPositionsAverages()
        {
            return operatorInputStats.entrySet().stream()
                    .collect(toMap(
                            Map.Entry::getKey,
                            entry -> (double) entry.getValue().getInputPositions() / operatorInputStats.get(entry.getKey()).getTotalDrivers()));
        }

        public Map<String, Double> getOperatorInputPositionsStdDevs()
        {
            return operatorInputStats.entrySet().stream()
                    .collect(toMap(
                            Map.Entry::getKey,
                            entry -> computedStdDev(
                                    entry.getValue().getSumSquaredInputPositions(),
                                    entry.getValue().getInputPositions(),
                                    entry.getValue().getTotalDrivers())));
        }

        public Map<String, Double> getOperatorHashCollisionsAverages()
        {
            return operatorHashCollisionsStats.entrySet().stream()
                    .collect(toMap(
                            Map.Entry::getKey,
                            entry -> entry.getValue().getWeightedHashCollisions() / operatorInputStats.get(entry.getKey()).getInputPositions()));
        }

        public Map<String, Double> getOperatorHashCollisionsStdDevs()
        {
            return operatorHashCollisionsStats.entrySet().stream()
                    .collect(toMap(
                            Map.Entry::getKey,
                            entry -> computedWeightedStdDev(
                                    entry.getValue().getWeightedSumSquaredHashCollisions(),
                                    entry.getValue().getWeightedHashCollisions(),
                                    operatorInputStats.get(entry.getKey()).getInputPositions())));
        }

        private static double computedWeightedStdDev(double sumSquared, double sum, double totalWeight)
        {
            double average = sum / totalWeight;
            double variance = (sumSquared - 2 * sum * average) / totalWeight + average * average;
            // variance might be negative because of numeric inaccuracy, therefore we need to use max
            return sqrt(max(variance, 0d));
        }

        public Map<String, Double> getOperatorExpectedCollisionsAverages()
        {
            return operatorHashCollisionsStats.entrySet().stream()
                    .collect(toMap(
                            Map.Entry::getKey,
                            entry -> entry.getValue().getWeightedExpectedHashCollisions() / operatorInputStats.get(entry.getKey()).getInputPositions()));
        }

        public Optional<WindowNodeStats> getWindowNodeStats()
        {
            return windowNodeStats;
        }

        public static PlanNodeStats merge(PlanNodeStats left, PlanNodeStats right)
        {
            checkArgument(left.getPlanNodeId().equals(right.getPlanNodeId()), "planNodeIds do not match. %s != %s", left.getPlanNodeId(), right.getPlanNodeId());

            long planNodeInputPositions = left.planNodeInputPositions + right.planNodeInputPositions;
            DataSize planNodeInputDataSize = succinctBytes(left.planNodeInputDataSize.toBytes() + right.planNodeInputDataSize.toBytes());
            long planNodeOutputPositions = left.planNodeOutputPositions + right.planNodeOutputPositions;
            DataSize planNodeOutputDataSize = succinctBytes(left.planNodeOutputDataSize.toBytes() + right.planNodeOutputDataSize.toBytes());

            Map<String, OperatorInputStats> operatorInputStats = mergeMaps(left.operatorInputStats, right.operatorInputStats, OperatorInputStats::merge);
            Map<String, OperatorHashCollisionsStats> operatorHashCollisionsStats = mergeMaps(left.operatorHashCollisionsStats, right.operatorHashCollisionsStats, OperatorHashCollisionsStats::merge);
            Optional<WindowNodeStats> windowNodeStats = WindowNodeStats.merge(left.windowNodeStats, right.windowNodeStats);
            return new PlanNodeStats(
                    left.getPlanNodeId(),
                    new Duration(left.getPlanNodeWallTime().toMillis() + right.getPlanNodeWallTime().toMillis(), MILLISECONDS),
                    planNodeInputPositions, planNodeInputDataSize,
                    planNodeOutputPositions, planNodeOutputDataSize,
                    operatorInputStats,
                    operatorHashCollisionsStats,
                    windowNodeStats);
        }
    }

    private static <K> Map<K, OperatorInputStats> mergeOperatorInputStatsMaps(Map<K, OperatorInputStats> map1, Map<K, OperatorInputStats> map2)
    {
        return mergeMaps(map1, map2, OperatorInputStats::merge);
    }

    private static <K> Map<K, OperatorHashCollisionsStats> mergeOperatorHashCollisionsStatsMaps(Map<K, OperatorHashCollisionsStats> map1, Map<K, OperatorHashCollisionsStats> map2)
    {
        return mergeMaps(map1, map2, OperatorHashCollisionsStats::merge);
    }

    private static <K, V> Map<K, V> mergeMaps(Map<K, V> map1, Map<K, V> map2, BinaryOperator<V> merger)
    {
        return Stream.of(map1, map2)
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        merger::apply));
    }

    private static class OperatorHashCollisionsStats
    {
        private final double weightedHashCollisions;
        private final double weightedSumSquaredHashCollisions;
        private final double weightedExpectedHashCollisions;

        public OperatorHashCollisionsStats(double weightedHashCollisions, double weightedSumSquaredHashCollisions, double weightedExpectedHashCollisions)
        {
            this.weightedHashCollisions = weightedHashCollisions;
            this.weightedSumSquaredHashCollisions = weightedSumSquaredHashCollisions;
            this.weightedExpectedHashCollisions = weightedExpectedHashCollisions;
        }

        public double getWeightedHashCollisions()
        {
            return weightedHashCollisions;
        }

        public double getWeightedSumSquaredHashCollisions()
        {
            return weightedSumSquaredHashCollisions;
        }

        public double getWeightedExpectedHashCollisions()
        {
            return weightedExpectedHashCollisions;
        }

        public static OperatorHashCollisionsStats merge(OperatorHashCollisionsStats first, OperatorHashCollisionsStats second)
        {
            return new OperatorHashCollisionsStats(
                    first.weightedHashCollisions + second.weightedHashCollisions,
                    first.weightedSumSquaredHashCollisions + second.weightedSumSquaredHashCollisions,
                    first.weightedExpectedHashCollisions + second.weightedExpectedHashCollisions);
        }
    }

    private static class WindowNodeStats
    {
        private final List<Long> indexSize;
        private final List<Long> partitionRowsPerIndex;
        private final List<Long> partitionRowsPerDriver;
        private final int activeDrivers;
        private final int totalDrivers;

        public static WindowNodeStats create(WindowInfo info)
        {
            ImmutableList.Builder<Long> indexSize = ImmutableList.builder();
            ImmutableList.Builder<Long> partitionRowsPerIndex = ImmutableList.builder();
            info.getWindowInfos().stream()
                    .map(SingleDriverWindowInfo::getIndexInfo)
                    .flatMap(Collection::stream)
                    .forEach(indexInfo -> {
                        indexSize.add(indexInfo.getSizeInBytes());
                        partitionRowsPerIndex.add(indexInfo.getTotalRowCountInPartitions());
                    });

            ImmutableList.Builder<Long> partitionRowsPerDriver = ImmutableList.builder();
            int activeDrivers = 0;
            int totalDrivers = 0;

            for (SingleDriverWindowInfo singleDriverWindowInfo : info.getWindowInfos()) {
                long partitionRowsCount = singleDriverWindowInfo.getIndexInfo().stream()
                        .mapToLong(IndexInfo::getTotalRowCountInPartitions)
                        .sum();
                partitionRowsPerDriver.add(partitionRowsCount);
                totalDrivers++;
                if (partitionRowsCount > 0) {
                    activeDrivers++;
                }
            }

            return new WindowNodeStats(indexSize.build(), partitionRowsPerIndex.build(), partitionRowsPerDriver.build(), activeDrivers, totalDrivers);
        }

        private WindowNodeStats(List<Long> indexSize, List<Long> partitionRowsPerIndex, List<Long> partitionRowsPerDriver, int activeDrivers, int totalDrivers)
        {
            this.indexSize = requireNonNull(indexSize, "indexSize is null");
            this.partitionRowsPerIndex = requireNonNull(partitionRowsPerIndex, "partitionRowsPerIndex is null");
            this.partitionRowsPerDriver = requireNonNull(partitionRowsPerDriver, "partitionRowsPerDriver is null");
            this.activeDrivers = activeDrivers;
            this.totalDrivers = totalDrivers;
        }

        public static Optional<WindowNodeStats> merge(Optional<WindowNodeStats> first, Optional<WindowNodeStats> second)
        {
            if (first.isPresent() && second.isPresent()) {
                return Optional.of(merge(first.get(), second.get()));
            }
            else if (first.isPresent()) {
                return first;
            }
            else if (second.isPresent()) {
                return second;
            }
            else {
                return Optional.empty();
            }
        }

        public static WindowNodeStats merge(WindowNodeStats first, WindowNodeStats second)
        {
            return new WindowNodeStats(
                    ImmutableList.copyOf(concat(first.indexSize, second.indexSize)),
                    ImmutableList.copyOf(concat(first.partitionRowsPerIndex, second.partitionRowsPerIndex)),
                    ImmutableList.copyOf(concat(first.partitionRowsPerDriver, second.partitionRowsPerDriver)),
                    first.activeDrivers + second.activeDrivers,
                    first.totalDrivers + second.totalDrivers);
        }

        public DistributionSnapshot getIndexSizeDistribution()
        {
            return createDistributionSnapshot(indexSize);
        }

        public DistributionSnapshot getPartitionRowsPerIndexDistribution()
        {
            return createDistributionSnapshot(partitionRowsPerIndex);
        }

        public DistributionSnapshot getPartitionRowsPerDriverDistribution()
        {
            return createDistributionSnapshot(partitionRowsPerDriver);
        }

        private static DistributionSnapshot createDistributionSnapshot(List<Long> values)
        {
            Distribution distribution = new Distribution();
            values.forEach(distribution::add);
            return distribution.snapshot();
        }

        public int getActiveDrivers()
        {
            return activeDrivers;
        }

        public int getTotalDrivers()
        {
            return totalDrivers;
        }
    }
}
