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

#include <memory>
#include <utility>

#include <LegacyOptimizer/ModelInferenceCompilationRule.hpp>

#ifdef NES_ENABLE_INFERENCE
#include <LogicalInferModelNameOperator.hpp>
#include <LogicalInferModelOperator.hpp>
#include <ModelCatalog.hpp>
#endif

namespace NES::LegacyOptimizer
{

#ifdef NES_ENABLE_INFERENCE
ModelInferenceCompilationRule::ModelInferenceCompilationRule(std::shared_ptr<const NES::Nebuli::Inference::ModelCatalog> modelCatalog)
#else
ModelInferenceCompilationRule::ModelInferenceCompilationRule([[maybe_unused]] std::shared_ptr<const void> modelCatalog)
#endif
#ifdef NES_ENABLE_INFERENCE
    : catalog(std::move(modelCatalog))
#endif
{
}

void ModelInferenceCompilationRule::apply([[maybe_unused]] LogicalPlan& queryPlan)
{
#ifdef NES_ENABLE_INFERENCE
    NES_DEBUG("ModelInferenceCompilationRule: Plan before\n{}", queryPlan);

    for (auto modelNameOperator : NES::getOperatorByType<InferModel::LogicalInferModelNameOperator>(queryPlan))
    {
        auto name = modelNameOperator.getModelName();
        auto model = catalog->load(name);
        USED_IN_DEBUG auto shouldReplace = replaceOperator(
            queryPlan, modelNameOperator, InferModel::LogicalInferModelOperator(model, modelNameOperator.getInputFields()));
        queryPlan = std::move(shouldReplace.value());
    }

    NES_DEBUG("ModelInferenceCompilationRule: Plan after\n{}", queryPlan);
#else
    // When inference is disabled, this rule is a no-op
    NES_DEBUG("ModelInferenceCompilationRule: Inference disabled, skipping plan modification");
#endif
}

}
