#pragma once

#include <Common/Logger.h>
#include <Common/MPMCQueue.h>
#include <Common/ThreadManager.h>
#include <Flash/Disaggregated/GRPCPageReceiverContext.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <kvproto/mpp.pb.h>

namespace DB
{
namespace DM
{
struct RemoteSegmentReadTask;
using RemoteSegmentReadTaskPtr = std::shared_ptr<RemoteSegmentReadTask>;
} // namespace DM

struct PageReceivedMessage
{
    String req_info;
    DM::RemoteSegmentReadTaskPtr seg_task;
    const TrackedPageDataPacketPtr packet;
    const mpp::Error * error_ptr;
    std::vector<const String *> chunks;

    PageReceivedMessage(
        const String & req_info_,
        const DM::RemoteSegmentReadTaskPtr & seg_task_,
        const TrackedPageDataPacketPtr & packet_,
        const mpp::Error * error_ptr_,
        std::vector<const String *> && chunks_)
        : req_info(req_info_)
        , seg_task(seg_task_)
        , packet(packet_)
        , error_ptr(error_ptr_)
        , chunks(std::move(chunks_))
    {
    }
};
using PageReceivedMessagePtr = std::shared_ptr<PageReceivedMessage>;

struct PageReceiverResult
{
    std::shared_ptr<tipb::SelectResponse> resp;
    size_t call_index;
    String req_info;
    bool meet_error;
    String error_msg;
    bool eof;
    DecodeDetail decode_detail;

    PageReceiverResult()
        : PageReceiverResult(nullptr, 0)
    {}

    static PageReceiverResult newOk(std::shared_ptr<tipb::SelectResponse> resp_, size_t call_index_, const String & req_info_)
    {
        return {resp_, call_index_, req_info_, /*meet_error*/ false, /*error_msg*/ "", /*eof*/ false};
    }

    static PageReceiverResult newEOF(const String & req_info_)
    {
        return {/*resp*/ nullptr, 0, req_info_, /*meet_error*/ false, /*error_msg*/ "", /*eof*/ true};
    }

    static PageReceiverResult newError(size_t call_index, const String & req_info, const String & error_msg)
    {
        return {/*resp*/ nullptr, call_index, req_info, /*meet_error*/ true, error_msg, /*eof*/ false};
    }

private:
    PageReceiverResult(
        std::shared_ptr<tipb::SelectResponse> resp_,
        size_t call_index_,
        const String & req_info_ = "",
        bool meet_error_ = false,
        const String & error_msg_ = "",
        bool eof_ = false)
        : resp(resp_)
        , call_index(call_index_)
        , req_info(req_info_)
        , meet_error(meet_error_)
        , error_msg(error_msg_)
        , eof(eof_)
    {}
};

template <typename RPCContext>
class PageReceiverBase
{
public:
    PageReceiverBase(
        std::unique_ptr<RPCContext> rpc_context_,
        size_t source_num_,
        size_t max_streams_,
        const String & req_id,
        const String & executor_id);

    ~PageReceivedMessage();

    void cancel();

    void close();

    PageReceiverResult nextResult(
        std::queue<Block> & block_queue,
        const Block & header,
        size_t stream_id,
        std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr);

private:
    using Request = typename RPCContext::Request;

    void readLoop();

    std::tuple<bool, String> taskReadLoop(Request && req);

    void setUpConnection();

    bool setEndState(ExchangeReceiverState new_state);
    String getStatusString();

    void connectionDone(
        bool meet_error,
        const String & local_err_msg,
        const LoggerPtr & log);

    void finishAllMsgChannels();
    void cancelAllMsgChannels();

    PageReceiverResult toDecodeResult(
        std::queue<Block> & block_queue,
        const Block & header,
        const std::shared_ptr<PageReceivedMessage> & recv_msg,
        std::unique_ptr<CHBlockChunkDecodeAndSquash> & decoder_ptr);

private:
    std::unique_ptr<RPCContext> rpc_context;
    const size_t source_num;
    const size_t max_buffer_size;

    std::shared_ptr<ThreadManager> thread_manager;
    std::vector<std::unique_ptr<MPMCQueue<PageReceivedMessagePtr>>> msg_channels;

    std::mutex mu;
    /// should lock `mu` when visit these members
    Int32 live_connections;
    ExchangeReceiverState state;
    String err_msg;

    bool collected;
    int thread_count;

    LoggerPtr exc_log;
};

class PageReceiver : public PageReceiverBase<GRPCPagesReceiverContext>
{
public:
    using Base = PageReceiverBase<GRPCPagesReceiverContext>;
    using Base::Base;
};

} // namespace DB
