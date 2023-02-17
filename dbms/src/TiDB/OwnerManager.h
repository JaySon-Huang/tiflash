#pragma once

#include <common/types.h>
#include <etcd/v3election.pb.h>

#include <thread>

namespace DB
{
class Context;
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;

namespace Etcd
{
class Client;
using ClientPtr = std::shared_ptr<Client>;
class Session;
using SessionPtr = std::shared_ptr<Session>;

using LeaseID = Int64;
using LeaderKey = v3electionpb::LeaderKey;
} // namespace Etcd

class OwnerManager;
using OwnerManagerPtr = std::unique_ptr<OwnerManager>;


class OwnerManager
{
public:
    static OwnerManagerPtr
    createS3GCOwner(
        Context & context,
        const String & id,
        const Etcd::ClientPtr & client,
        Int64 owner_ttl = 60)
    {
        return std::make_unique<OwnerManager>(context, "/tiflash/s3gc/owner", id, client, owner_ttl);
    }

    OwnerManager(
        Context & context,
        const String & campaign_name_,
        const String & id_,
        const Etcd::ClientPtr & client_,
        Int64 owner_ttl = 60);

    ~OwnerManager();

    void campaignOwner();

    bool isOwner();

    void resignOwner();

    enum OwnerResultType
    {
        Ok,
        NoLeader,
        GrpcError,
    };
    struct OwnerIDResult
    {
        OwnerResultType type;
        // If type == Ok, s is the OwnerID
        // If type == NoLeader, s is empty string
        // If type == GrpcError, s is the error message
        String s;
    };
    OwnerIDResult getOwnerID();

    void cancel();

    void campaignCancel();

    void setBeOwnerHook(std::function<void()> && hook)
    {
        be_owner = hook;
    }

private:
    void camaignLoop(Etcd::SessionPtr session);

    std::optional<String> getOwnerInfo(const String & check_id);

    void toBeOwner(Etcd::LeaderKey && leader_key);

    void watchOwner(const Etcd::SessionPtr & session, const String & owner_key);
    void retireOwner();

    void revokeEtcdSession(Etcd::LeaseID lease_id);

private:
    String campaign_name;
    String id;
    Etcd::ClientPtr client;
    Int64 leader_ttl;

    std::atomic<bool> enable_camaign{true};
    std::thread th_camaign;
    std::mutex mtx_leader;
    Etcd::LeaderKey leader;

    std::function<void()> be_owner;

    Context & global_ctx;
    LoggerPtr log;
};
} // namespace DB
