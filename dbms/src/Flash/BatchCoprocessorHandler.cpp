#include <Flash/BatchCoprocessorHandler.h>

#include <Flash/Coprocessor/DAGDriver.h>
#include <Flash/Coprocessor/InterpreterDAG.h>
#include <Storages/IStorage.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/RegionException.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

BatchCoprocessorHandler::BatchCoprocessorHandler(
    CoprocessorContext & cop_context_, const coprocessor::BatchRequest * cop_request_, coprocessor::BatchResponse * cop_response_)
    : cop_context(cop_context_), cop_request(cop_request_), cop_response(cop_response_), log(&Logger::get("BatchCoprocessorHandler"))
{}

grpc::Status BatchCoprocessorHandler::execute()
try
{
    switch (cop_request->tp())
    {
        case COP_REQ_TYPE_DAG:
        {
            std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> key_ranges;
            tipb::DAGRequest dag_request;
            dag_request.ParseFromString(cop_request->data());
            LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handling DAG request: " << dag_request.DebugString());
            tipb::SelectResponse dag_response;
            std::vector<RegionInfo> regions;
            for (auto & r : cop_request->regions()) {
                regions.emplace_back(r.region_id(), r.region_epoch().version(), r.region_epoch().conf_ver());
            }
            DAGDriver driver(cop_context.db_context, dag_request, regions, dag_request.start_ts_fallback(), -1, std::move(key_ranges),
                dag_response);
            driver.execute();
            cop_response->set_data(dag_response.SerializeAsString());
            LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handle DAG request done");
            break;
        }
        case COP_REQ_TYPE_ANALYZE:
        case COP_REQ_TYPE_CHECKSUM:
        default:
            throw Exception(
                "Coprocessor request type " + std::to_string(cop_request->tp()) + " is not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }
    return grpc::Status::OK;
}
catch (const LockException & e)
{
    LOG_ERROR(log,
        __PRETTY_FUNCTION__ << ": LockException: region " << cop_request->context().region_id() << "\n"
                            << e.getStackTrace().toString());
    cop_response->Clear();
    for (int i = 0; i < cop_request->regions_size(); i++) {
        auto * status = cop_response->add_region_status();
        if (cop_request->regions(i).region_id() == e.region_id)
        {
            auto * lock_info = status->mutable_locked();
            lock_info->set_key(e.lock_infos[0]->key);
            lock_info->set_primary_lock(e.lock_infos[0]->primary_lock);
            lock_info->set_lock_ttl(e.lock_infos[0]->lock_ttl);
            lock_info->set_lock_version(e.lock_infos[0]->lock_version);
        }
        else
        {
            status->set_success(true);
        }
    }
    // return ok so TiDB has the chance to see the LockException
    return grpc::Status::OK;
}
catch (const RegionException & e)
{
    LOG_ERROR(log,
        __PRETTY_FUNCTION__ << ": RegionException: region " << cop_request->context().region_id() << "\n"
                            << e.getStackTrace().toString());
    cop_response->Clear();
    //errorpb::Error * region_err;
    switch (e.status)
    {
        case RegionException::RegionReadStatus::NOT_FOUND:
        case RegionException::RegionReadStatus::PENDING_REMOVE:
            for (int i = 0; i < cop_request->regions_size(); i++) {
                auto * status = cop_response->add_region_status();
                status->mutable_region_error()->mutable_region_not_found()->set_region_id(cop_request->regions(i).region_id());
            }
            break;
        case RegionException::RegionReadStatus::VERSION_ERROR:
            for (int i = 0; i < cop_request->regions_size(); i++) {
                auto * status = cop_response->add_region_status();
                status->mutable_region_error()->mutable_epoch_not_match();
            }
            break;
        default:
            // should not happen
            break;
    }
    // return ok so TiDB has the chance to see the LockException
    return grpc::Status::OK;
}
catch (const Exception & e)
{
    LOG_ERROR(log, __PRETTY_FUNCTION__ << ": DB Exception: " << e.message() << "\n" << e.getStackTrace().toString());
    return recordError(e.code() == ErrorCodes::NOT_IMPLEMENTED ? grpc::StatusCode::UNIMPLEMENTED : grpc::StatusCode::INTERNAL, e.message());
}
catch (const pingcap::Exception & e)
{
    LOG_ERROR(log, __PRETTY_FUNCTION__ << ": KV Client Exception: " << e.message());
    return recordError(e.code() == ErrorCodes::NOT_IMPLEMENTED ? grpc::StatusCode::UNIMPLEMENTED : grpc::StatusCode::INTERNAL, e.message());
}
catch (const std::exception & e)
{
    LOG_ERROR(log, __PRETTY_FUNCTION__ << ": std exception: " << e.what());
    return recordError(grpc::StatusCode::INTERNAL, e.what());
}
catch (...)
{
    LOG_ERROR(log, __PRETTY_FUNCTION__ << ": other exception");
    return recordError(grpc::StatusCode::INTERNAL, "other exception");
}

grpc::Status BatchCoprocessorHandler::recordError(grpc::StatusCode err_code, const String & err_msg)
{
    cop_response->Clear();
    cop_response->set_other_error(err_msg);

    return grpc::Status(err_code, err_msg);
}

} // namespace DB
