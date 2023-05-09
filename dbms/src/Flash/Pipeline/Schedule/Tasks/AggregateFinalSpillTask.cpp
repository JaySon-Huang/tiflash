// Copyright 2023 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Flash/Pipeline/Schedule/Tasks/AggregateFinalSpillTask.h>
#include <Operators/AggregateContext.h>

namespace DB
{
AggregateFinalSpillTask::AggregateFinalSpillTask(
    MemoryTrackerPtr mem_tracker_,
    const String & req_id,
    PipelineExecutorStatus & exec_status_,
    const EventPtr & event_,
    AggregateContextPtr agg_context_,
    size_t index_)
    : IOEventTask(std::move(mem_tracker_), req_id, exec_status_, event_)
    , agg_context(std::move(agg_context_))
    , index(index_)
{
    assert(agg_context);
}

void AggregateFinalSpillTask::doFinalizeImpl()
{
    agg_context.reset();
}

ExecTaskStatus AggregateFinalSpillTask::doExecuteIOImpl()
{
    agg_context->spillData(index);
    return ExecTaskStatus::FINISHED;
}

} // namespace DB