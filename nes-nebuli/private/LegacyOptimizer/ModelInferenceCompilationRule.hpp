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

#pragma once

#include <memory>
#include <Plans/LogicalPlan.hpp>
#ifdef NES_ENABLE_INFERENCE
#include <ModelCatalog.hpp>
#endif

namespace NES::LegacyOptimizer
{

class ModelInferenceCompilationRule
{
public:
#ifdef NES_ENABLE_INFERENCE
    explicit ModelInferenceCompilationRule(std::shared_ptr<const NES::Nebuli::Inference::ModelCatalog> modelCatalog);
#else
    explicit ModelInferenceCompilationRule(std::shared_ptr<const void> modelCatalog);
#endif
    void apply(LogicalPlan& queryPlan);

private:
#ifdef NES_ENABLE_INFERENCE
    std::shared_ptr<const NES::Nebuli::Inference::ModelCatalog> catalog;
#endif
};
}
