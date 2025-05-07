/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Phases/LowerToCompiledQueryPlanPhase.hpp>

#include <memory>
#include <optional>
#include <ranges>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <Configuration/WorkerConfiguration.hpp>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatterProvider.hpp>
#include <InputFormatters/InputFormatterTask.hpp>
#include <Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/DumpMode.hpp>
#include <Util/ExecutionMode.hpp>
#include <Util/Strings.hpp>
#include <CompiledQueryPlan.hpp>
#include <ErrorHandling.hpp>
#include <ExecutablePipelineStage.hpp>
#include <Pipeline.hpp>
#include <PipelinedQueryPlan.hpp>
#include <SinkPhysicalOperator.hpp>
#include <SourcePhysicalOperator.hpp>
#include <options.hpp>

namespace NES
{

LowerToCompiledQueryPlanPhase::Successor
LowerToCompiledQueryPlanPhase::processSuccessor(const Predecessor& predecessor, const std::shared_ptr<Pipeline>& pipeline)
{
    PRECONDITION(pipeline->isSinkPipeline() || pipeline->isOperatorPipeline(), "expected a Sink or OperatorPipeline");

    if (pipeline->isSinkPipeline())
    {
        processSink(predecessor, pipeline);
        return {};
    }
    return processOperatorPipeline(pipeline);
}

void LowerToCompiledQueryPlanPhase::processSource(const std::shared_ptr<Pipeline>& pipeline)
/// This functions injects a formatter in between sources and its successor pipelines.
/// The formatter is chosen based on the SourceDescriptor ParserConfig.
/// If the `raw` input formatter is used, no additional pipelines are injected.
/// The Return values of this function is the list of successors which the source should emit to.
std::vector<std::weak_ptr<ExecutablePipeline>> injectFormatter(
    const std::shared_ptr<Pipeline>& pipeline,
    const SourcePhysicalOperator& sourceOperator,
    const std::shared_ptr<PipelinedQueryPlan>& pipelineQueryPlan,
    LoweringContext& loweringContext)
{
    const auto descriptor = sourceOperator.getDescriptor();
    if (NES::Util::toLowerCase(descriptor.getParserConfig().parserType) == "raw")
    {
        std::vector<std::weak_ptr<ExecutablePipeline>> executableSuccessorPipelines;
        for (const auto& successor : pipeline->getSuccessors())
        {
            if (auto executableSuccessor = processSuccessor(sourceOperator.getOriginId(), successor, pipelineQueryPlan, loweringContext))
            {
                executableSuccessorPipelines.emplace_back(*executableSuccessor);
            }
        }
        return executableSuccessorPipelines;
    }

    /// Inject a formatter
    auto inputFormatter = NES::InputFormatters::InputFormatterProvider::provideInputFormatter(
        descriptor.getParserConfig().parserType,
        *descriptor.getLogicalSource().getSchema(),
        descriptor.getParserConfig().tupleDelimiter,
        descriptor.getParserConfig().fieldDelimiter);

    auto inputFormatterTask
        = std::make_unique<InputFormatters::InputFormatterTask>(sourceOperator.getOriginId(), std::move(inputFormatter));
    auto executableInputFormatterPipeline = ExecutablePipeline::create(pipeline->getPipelineId(), std::move(inputFormatterTask), {});

    std::vector<std::weak_ptr<ExecutablePipeline>> executableSuccessorPipelines;
    for (const auto& successor : pipeline->getSuccessors())
    {
        if (auto executableSuccessor = processSuccessor(executableInputFormatterPipeline, successor, pipelineQueryPlan, loweringContext))
        {
            executableSuccessorPipelines.emplace_back(*executableSuccessor);
        }
    }
    executableInputFormatterPipeline->successors = std::move(executableSuccessorPipelines);
    loweringContext.pipelineToExecutableMap.emplace(executableInputFormatterPipeline->id, executableInputFormatterPipeline);

    return {executableInputFormatterPipeline};
}

void processSource(
    const std::shared_ptr<Pipeline>& pipeline,
    const std::shared_ptr<PipelinedQueryPlan>& pipelineQueryPlan,
    LoweringContext& loweringContext)
{
    PRECONDITION(pipeline->isSourcePipeline(), "expected a SourcePipeline {}", *pipeline);

    /// Convert logical source descriptor to actual source descriptor
    const auto sourceOperator = pipeline->getRootOperator().get<SourcePhysicalOperator>();

    const std::vector<std::shared_ptr<ExecutablePipeline>> executableSuccessorPipelines;
    auto inputFormatterTaskPipeline = provideInputFormatterTask(
        *sourceOperator.getDescriptor().getLogicalSource().getSchema(), sourceOperator.getDescriptor().getParserConfig());

    auto executableInputFormatterPipeline
        = ExecutablePipeline::create(pipeline->getPipelineId(), std::move(inputFormatterTaskPipeline), executableSuccessorPipelines);

    for (const auto& successor : pipeline->getSuccessors())
    {
        if (auto executableSuccessor = processSuccessor(executableInputFormatterPipeline, successor))
        {
            executableInputFormatterPipeline->successors.emplace_back(*executableSuccessor);
        }
    }

    /// Insert the executable pipeline into the pipelineQueryPlan at position 1 (after the source)
    pipelineQueryPlan->removePipeline(*pipeline);

    std::vector<std::weak_ptr<ExecutablePipeline>> inputFormatterTasks;

    pipelineToExecutableMap.emplace(getNextPipelineId(), executableInputFormatterPipeline);
    inputFormatterTasks.emplace_back(executableInputFormatterPipeline);

    sources.emplace_back(sourceOperator.getOriginId(), sourceOperator.id, sourceOperator.getDescriptor(), std::move(inputFormatterTasks));
    loweringContext.sources.emplace_back(
        sourceOperator.getOriginId(),
        sourceOperator.getDescriptor(),
        injectFormatter(pipeline, sourceOperator, pipelineQueryPlan, loweringContext));
}

void LowerToCompiledQueryPlanPhase::processSink(const Predecessor& predecessor, const std::shared_ptr<Pipeline>& pipeline)
{
    const auto sinkOperator = pipeline->getRootOperator().get<SinkPhysicalOperator>().getDescriptor();
    auto it = std::ranges::find(sinks, pipeline->getPipelineId(), &CompiledQueryPlan::Sink::id);
    if (it == sinks.end())
    {
        sinks.emplace_back(pipeline->getPipelineId(), sinkOperator, std::vector<Predecessor>{});
        it = sinks.end() - 1;
    }
    it->predecessor.emplace_back(predecessor);
}

std::unique_ptr<ExecutablePipelineStage> LowerToCompiledQueryPlanPhase::getStage(const std::shared_ptr<Pipeline>& pipeline)
{
    nautilus::engine::Options options;
    /// We disable multithreading in MLIR by default to not interfere with NebulaStream's thread model
    options.setOption("mlir.enableMultithreading", false);
    switch (pipelineQueryPlan->getExecutionMode())
    {
        case ExecutionMode::COMPILER: {
            options.setOption("engine.Compilation", true);
            break;
        }
        case ExecutionMode::INTERPRETER: {
            options.setOption("engine.Compilation", false);
            break;
        }
        default: {
            INVARIANT(false, "Invalid backend");
        }
    }
    /// See: https://github.com/nebulastream/nautilus/blob/main/docs/options.md
    switch (dumpQueryCompilationIntermediateRepresentations)
    {
        case DumpMode::NONE:
            options.setOption("dump.all", false);
            options.setOption("dump.console", false);
            options.setOption("dump.file", false);
            break;
        case DumpMode::CONSOLE:
            options.setOption("dump.all", true);
            options.setOption("dump.console", true);
            options.setOption("dump.file", false);
            break;
        case DumpMode::FILE:
            options.setOption("dump.all", true);
            options.setOption("dump.console", false);
            options.setOption("dump.file", true);
            break;
        case DumpMode::FILE_AND_CONSOLE:
            options.setOption("dump.all", true);
            options.setOption("dump.console", true);
            options.setOption("dump.file", true);
            break;
    }
    return std::make_unique<CompiledExecutablePipelineStage>(pipeline, pipeline->getOperatorHandlers(), options);
}

std::shared_ptr<ExecutablePipeline> LowerToCompiledQueryPlanPhase::processOperatorPipeline(const std::shared_ptr<Pipeline>& pipeline)
{
    /// check if the particular pipeline already exist in the pipeline map.
    if (const auto executable = pipelineToExecutableMap.find(pipeline->getPipelineId()); executable != pipelineToExecutableMap.end())
    {
        return executable->second;
    }
    auto executablePipeline = ExecutablePipeline::create(PipelineId(pipeline->getPipelineId()), getStage(pipeline), {});

    for (const auto& successor : pipeline->getSuccessors())
    {
        if (auto executableSuccessor = processSuccessor(executablePipeline, successor))
        {
            executablePipeline->successors.emplace_back(*executableSuccessor);
        }
    }

    pipelineToExecutableMap.emplace(pipeline->getPipelineId(), executablePipeline);
    return executablePipeline;
}

std::unique_ptr<CompiledQueryPlan> LowerToCompiledQueryPlanPhase::apply(const std::shared_ptr<PipelinedQueryPlan>& pipelineQueryPlan)
{
    this->pipelineQueryPlan = pipelineQueryPlan;

    /// Process all pipelines recursively.
    for (auto sourcePipelines = pipelineQueryPlan->getSourcePipelines(); const auto& pipeline : sourcePipelines)
    {
        processSource(pipeline);
    }

    auto pipelines = std::move(pipelineToExecutableMap) | std::views::values | std::ranges::to<std::vector>();

    return CompiledQueryPlan::create(pipelineQueryPlan->getQueryId(), std::move(pipelines), std::move(sinks), std::move(sources));
}

}
