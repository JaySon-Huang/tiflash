
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/ThreadFactory.h>
#include <Interpreters/Context.h>
#include <TiDB/Etcd/Client.h>
#include <TiDB/OwnerManager.h>
#include <common/logger_useful.h>
#include <etcd/v3election.grpc.pb.h>

#include <chrono>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <grpcpp/client_context.h>
#include <grpcpp/impl/codegen/call_op_set.h>
#include <grpcpp/support/sync_stream.h>
#ifdef __clang__
#pragma clang diagnostic pop
#endif

namespace DB
{

OwnerManager::OwnerManager(
    Context & context,
    const String & campaign_name_,
    const String & id_,
    const Etcd::ClientPtr & client_,
    Int64 owner_ttl)
    : campaign_name(campaign_name_)
    , id(id_)
    , client(client_)
    , leader_ttl(owner_ttl)
    , global_ctx(context.getGlobalContext())
    , log(Logger::get(fmt::format("name:{} id:{}", campaign_name, id)))
{
}

OwnerManager::~OwnerManager()
{
    campaignCancel();
}

void OwnerManager::cancel()
{
    enable_camaign = false;
}

void OwnerManager::campaignCancel()
{
    cancel();
    if (th_camaign.joinable())
    {
        th_camaign.join();
    }
}

void OwnerManager::campaignOwner()
{
    auto session = client->createSession(global_ctx, leader_ttl, Etcd::InvalidLeaseID);
    LOG_INFO(log, "start campaign owner");
    th_camaign = ThreadFactory::newThread(
        false,
        campaign_name,
        [this, s = std::move(session)] {
            camaignLoop(s);
        });
}

void OwnerManager::camaignLoop(Etcd::SessionPtr session)
{
    try
    {
        while (true)
        {
            if (session->isCanceled())
            {
                LOG_INFO(log, "etcd session is canceled, create a new one");
                auto old_lease_id = session->leaseID();
                // Start a new session
                session = client->createSession(global_ctx, leader_ttl, Etcd::InvalidLeaseID);
                // TODO if create new session failed, revoke session and break
                UNUSED(old_lease_id);
            }
            if (!enable_camaign)
            {
                LOG_INFO(log, "break campaign loop, disabled");
                revokeEtcdSession(session->leaseID());
                break;
            }

            const auto lease_id = session->leaseID();
            Etcd::LeaderKey new_leader;
            grpc::Status status;
            std::tie(new_leader, status) = client->campaign(campaign_name, id, lease_id);
            if (!status.ok())
            {
                // if error, continue next campaign
                LOG_INFO(
                    log,
                    "failed to campaign, id={} lease={} code={} msg={}",
                    id,
                    lease_id,
                    status.error_code(),
                    status.error_message());
                continue;
            }

            auto owner_key = getOwnerInfo(id);
            if (!owner_key)
            {
                // if error, continue
                continue;
            }

            // become owner
            toBeOwner(std::move(new_leader));
            LOG_INFO(log, "become the owner with lease={:x}", lease_id);

            // waits until
            watchOwner(session, owner_key.value());
            retireOwner();

            LOG_WARNING(log, "is not the owner");
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "OwnerManager::camaignLoop");
    }
}

void OwnerManager::toBeOwner(Etcd::LeaderKey && leader_key)
{
    RUNTIME_CHECK(!leader_key.name().empty(), leader_key.ShortDebugString());

    {
        std::lock_guard lk(mtx_leader);
        leader.Swap(&leader_key);
    }

    if (be_owner)
        be_owner();
}

void OwnerManager::watchOwner(const Etcd::SessionPtr & session, const String & owner_key)
{
    grpc::ClientContext grpc_ctx;
    // client->waitsUntilDeleted(&grpc_ctx, owner_key);
    auto reader = client->observe(&grpc_ctx, campaign_name);
    v3electionpb::LeaderResponse resp;
    while (true)
    {
        if (!enable_camaign)
            break;
        if (session->isCanceled())
            break;

        if (auto ok = reader->Read(&resp); ok)
        {
            const auto & kv = resp.kv();
            LOG_INFO(log, "watch receive, key={}", kv.key());
            continue;
        }
        LOG_INFO(log, "watcher is closed, no owner");
        auto status = reader->Finish();
        LOG_INFO(log, "watcher finished, code={} msg={}", status.error_code(), status.error_message());
        break;
    }
}

std::optional<String> OwnerManager::getOwnerInfo(const String & check_id)
{
    const auto & [kv, status] = client->leader(campaign_name);
    if (!status.ok())
    {
        LOG_INFO(log, "failed to get leader, code={} msg={}", status.error_code(), status.error_message());
        return std::nullopt;
    }
    const auto & owner_id = kv.value();
    if (owner_id != check_id)
    {
        LOG_WARNING(log, "is not the owner");
        return std::nullopt;
    }
    return kv.value();
}

bool OwnerManager::isOwner()
{
    std::lock_guard lk(mtx_leader);
    return !leader.name().empty();
}

void OwnerManager::retireOwner()
{
    std::lock_guard lk(mtx_leader);
    leader.Clear();
}

void OwnerManager::resignOwner()
{
    std::lock_guard lk(mtx_leader);
    if (leader.name().empty())
        return;
    client->resign(std::move(leader));
    cancel();
    leader.Clear();
    // resign success
    LOG_WARNING(log, "resign owner success");
}

void OwnerManager::revokeEtcdSession(Etcd::LeaseID lease_id)
{
    // revoke the session lease
    // if revoke takes longer than the ttl, lease is expired anyway.
    auto status = client->leaseRevoke(lease_id);
    LOG_INFO(log, "revoke session, code={} msg={}", status.error_code(), status.error_message());
}

OwnerManager::OwnerIDResult OwnerManager::getOwnerID()
{
    const auto & [val, status] = client->getFirstKey(campaign_name);
    if (!status.ok())
        return OwnerIDResult{
            .type = OwnerResultType::GrpcError,
            .s = fmt::format("code={} msg={}", status.error_code(), status.error_message()),
        };
    if (val.empty())
        return OwnerIDResult{
            .type = OwnerResultType::NoLeader,
            .s = "",
        };
    return OwnerIDResult{
        .type = OwnerResultType::Ok,
        .s = val,
    };
}
} // namespace DB
