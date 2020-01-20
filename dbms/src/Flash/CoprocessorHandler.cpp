#include <Flash/Coprocessor/DAGDriver.h>
#include <Flash/Coprocessor/InterpreterDAG.h>
#include <Flash/CoprocessorHandler.h>
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

CoprocessorHandler::CoprocessorHandler(
    CoprocessorContext & cop_context_, const coprocessor::Request * cop_request_, coprocessor::Response * cop_response_)
    : cop_context(cop_context_), cop_request(cop_request_), cop_response(cop_response_), log(&Logger::get("CoprocessorHandler"))
{}

grpc::Status CoprocessorHandler::execute()
try
{
    switch (cop_request->tp())
    {
        case COP_REQ_TYPE_DAG:
        {
            std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> key_ranges;
            std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> dup_key_ranges;
            for (auto & range : cop_request->ranges())
            {
                {
                    std::string start_key(range.start());
                    DecodedTiKVKey start(std::move(start_key));
                    std::string end_key(range.end());
                    DecodedTiKVKey end(std::move(end_key));
                    key_ranges.emplace_back(std::make_pair(std::move(start), std::move(end)));
                }
                {
                    std::string start_key(range.start());
                    DecodedTiKVKey start(std::move(start_key));
                    std::string end_key(range.end());
                    DecodedTiKVKey end(std::move(end_key));
                    dup_key_ranges.emplace_back(std::make_pair(std::move(start), std::move(end)));
                }
            }
            tipb::DAGRequest dag_request;
            dag_request.ParseFromString(cop_request->data());
            LOG_DEBUG(log, __PRETTY_FUNCTION__ << ": Handling DAG request: " << dag_request.DebugString());
            if (dag_request.has_is_rpn_expr() && dag_request.is_rpn_expr())
                throw Exception("DAG request with rpn expression is not supported in TiFlash", ErrorCodes::NOT_IMPLEMENTED);

            tipb::SelectResponse dag_response;
            DAGDriver driver(cop_context.db_context, dag_request, cop_context.kv_context.region_id(),
                cop_context.kv_context.region_epoch().version(), cop_context.kv_context.region_epoch().conf_ver(),
                cop_request->start_ts() > 0 ? cop_request->start_ts() : dag_request.start_ts_fallback(), cop_request->schema_ver(),
                std::move(key_ranges), dag_response);
            driver.execute();

            if (true)
            {
                tipb::SelectResponse tmt_dag_response;
                auto tmt_dag_request = dag_request;
                auto orig_table_id = dag_request.mutable_executors(0)->tbl_scan().table_id();
                tmt_dag_request.mutable_executors(0)->mutable_tbl_scan()->set_table_id(orig_table_id + 1000000);
                DAGDriver tmt_driver(cop_context.db_context, tmt_dag_request, cop_context.kv_context.region_id(),
                    cop_context.kv_context.region_epoch().version(), cop_context.kv_context.region_epoch().conf_ver(),
                    cop_request->start_ts() > 0 ? cop_request->start_ts() : dag_request.start_ts_fallback(), cop_request->schema_ver(),
                    std::move(dup_key_ranges), tmt_dag_response);
                tmt_driver.execute();
                // compare their output
                const String dm_out = dag_response.ShortDebugString();
                const String tmt_out = tmt_dag_response.ShortDebugString();
                LOG_WARNING(log, "dm_output:" << dm_out);
                LOG_WARNING(log, "tmt_output:" << tmt_out);
                if (dm_out != tmt_out)
                {
                    LOG_ERROR(log, "dm_output is not align with tmt_output!");
                }
            }

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
    LOG_WARNING(
        log, __PRETTY_FUNCTION__ << ": LockException: region " << cop_request->context().region_id() << ", message: " << e.message());
    cop_response->Clear();
    kvrpcpb::LockInfo * lock_info = cop_response->mutable_locked();
    lock_info->set_key(e.lock_infos[0]->key);
    lock_info->set_primary_lock(e.lock_infos[0]->primary_lock);
    lock_info->set_lock_ttl(e.lock_infos[0]->lock_ttl);
    lock_info->set_lock_version(e.lock_infos[0]->lock_version);
    // return ok so TiDB has the chance to see the LockException
    return grpc::Status::OK;
}
catch (const RegionException & e)
{
    LOG_WARNING(
        log, __PRETTY_FUNCTION__ << ": RegionException: region " << cop_request->context().region_id() << ", message: " << e.message());
    cop_response->Clear();
    errorpb::Error * region_err;
    switch (e.status)
    {
        case RegionException::RegionReadStatus::NOT_FOUND:
        case RegionException::RegionReadStatus::PENDING_REMOVE:
            region_err = cop_response->mutable_region_error();
            region_err->mutable_region_not_found()->set_region_id(cop_request->context().region_id());
            break;
        case RegionException::RegionReadStatus::VERSION_ERROR:
            region_err = cop_response->mutable_region_error();
            region_err->mutable_epoch_not_match();
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

grpc::Status CoprocessorHandler::recordError(grpc::StatusCode err_code, const String & err_msg)
{
    cop_response->Clear();
    cop_response->set_other_error(err_msg);

    return grpc::Status(err_code, err_msg);
}

} // namespace DB
