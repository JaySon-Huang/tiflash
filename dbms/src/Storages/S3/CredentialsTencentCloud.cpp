// Copyright 2026 PingCAP, Inc.
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

#include <Storages/S3/Credentials.h>
#include <Storages/S3/CredentialsAliCloud.h>
#include <Storages/S3/CredentialsTencentCloud.h>
#include <Storages/S3/PocoHTTPClientFactory.h>
#include <Storages/S3/S3Common.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/utils/UUID.h>
#include <aws/core/utils/ratelimiter/DefaultRateLimiter.h>
#include <common/logger_useful.h>


#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#pragma GCC diagnostic pop

namespace DB::S3::TencentCloud
{
std::shared_ptr<Aws::Auth::AWSCredentialsProvider> TencentCloudSTSAssumeRoleWebIdentityCredentialsProvider::build(
    const Aws::Client::ClientConfiguration & cfg)
{
    // check environment variables
    auto region = Aws::Environment::GetEnv("TKE_REGION");
    if (region.empty())
    {
        region = Aws::Environment::GetEnv("TKE_DEFAULT_REGION");
    }
    auto role_arn = Aws::Environment::GetEnv("TKE_ROLE_ARN");
    auto provider_id = Aws::Environment::GetEnv("TKE_PROVIDER_ID");
    auto token_file = Aws::Environment::GetEnv("TKE_WEB_IDENTITY_TOKEN_FILE");

    // only support environment variable
    if (role_arn.empty() || token_file.empty() || provider_id.empty() || region.empty())
    {
        LOG_INFO(
            Logger::get(),
            "Environment variables must be specified to use Tencent Cloud STS AssumeRole web identity creds provider.");
        return nullptr;
    }
    return std::make_shared<TencentCloudSTSAssumeRoleWebIdentityCredentialsProvider>(cfg, region, role_arn, provider_id, token_file);
}

TencentCloudSTSAssumeRoleWebIdentityCredentialsProvider::TencentCloudSTSAssumeRoleWebIdentityCredentialsProvider(
    const Aws::Client::ClientConfiguration & cfg,
    const String & region_id,
    const String & role_arn,
    const String & provider_id,
    const String & token_file)
    : m_cfg(cfg)
    , m_region(region_id)
    , m_role_arn(role_arn)
    , m_provider_id(provider_id)
    , m_token_file(token_file)
    , m_initialized(false)
    , log(Logger::get())
{
    m_session_name = Aws::Environment::GetEnv("TKE_ROLE_SESSION_NAME");
    if (m_session_name.empty())
    {
        m_session_name = Aws::String("tiflash-") + static_cast<Aws::String>(Aws::Utils::UUID::RandomUUID());
    }
    else
    {
        LOG_DEBUG(log, "Resolved session_name from environment variable to be {}", m_session_name);
    }

    m_endpoint = fmt::format("https://sts.{}.tencentcloudapi.com", m_region);
    auto poco_cfg = PocoHTTPClientConfiguration(std::make_shared<RemoteHostFilter>(), 1, false, true);
    // use default retry strategy in poco http client
    m_http_client_factory = std::make_shared<PocoHTTPClientFactory>(poco_cfg);
    m_limiter = Aws::MakeShared<Aws::Utils::RateLimits::DefaultRateLimiter<>>(
        "TencentCloudSTSAssumeRoleWebIdentityCredentialsProvider",
        200000);
    m_initialized = true;
    LOG_INFO(log, "Creating Tencent Cloud STS AssumeRole with web identity creds provider.");
}

Aws::String TencentCloudSTSAssumeRoleWebIdentityCredentialsProvider::calculateRequestBody() const
{
    Aws::Utils::Json::JsonValue request_body;
    request_body.WithString("ProviderId", m_provider_id);
    request_body.WithString("WebIdentityToken", m_token);
    request_body.WithString("RoleArn", m_role_arn);
    request_body.WithString("RoleSessionName", m_session_name);
    return request_body.View().WriteCompact();
}

Aws::Auth::AWSCredentials TencentCloudSTSAssumeRoleWebIdentityCredentialsProvider::GetAWSCredentials()
{
    // A valid client means required information like role arn and token file were constructed correctly.
    // We can use this provider to load creds, otherwise, we can just return empty creds.
    if (!m_initialized)
    {
        return Aws::Auth::AWSCredentials();
    }
    refreshIfExpired();
    Aws::Utils::Threading::ReaderLockGuard guard(m_reloadLock);
    return m_credentials;
}

void TencentCloudSTSAssumeRoleWebIdentityCredentialsProvider::Reload()
{
    LOG_INFO(
        log,
        "Credentials have expired, attempting to renew from Tencent Cloud STS, role_arn={} role_session_name={}.",
        m_role_arn,
        m_session_name);

    std::ifstream token_file(m_token_file.c_str());
    if (token_file)
    {
        Aws::String token((std::istreambuf_iterator<char>(token_file)), std::istreambuf_iterator<char>());
        m_token = token;
    }
    else
    {
        LOG_ERROR(log, "Can't open token file: {}", m_token_file);
        return;
    }

    const auto now_sec = std::to_string(Aws::Utils::DateTime::Now().Seconds());
    Aws::String request_body = calculateRequestBody();

    // create http request
    auto http_client = m_http_client_factory->CreateHttpClient(Aws::Client::ClientConfiguration());
    auto http_request = m_http_client_factory->CreateHttpRequest(
        m_endpoint,
        Aws::Http::HttpMethod::HTTP_POST,
        Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
    auto body_stream = Aws::MakeShared<std::stringstream>("TencentCloudSTSAssumeRoleWebIdentityCredentialsProvider");
    (*body_stream) << request_body;
    http_request->AddContentBody(body_stream);
    http_request->SetHeaderValue("Host", fmt::format("sts.{}.tencentcloudapi.com", m_region));
    http_request->SetHeaderValue("Content-Type", "application/json; charset=utf-8");
    http_request->SetHeaderValue("Authorization", "SKIP");
    http_request->SetHeaderValue("X-TC-Action", "AssumeRoleWithWebIdentity");
    http_request->SetHeaderValue("X-TC-Version", "2018-08-13");
    http_request->SetHeaderValue("X-TC-Timestamp", now_sec);
    http_request->SetHeaderValue("X-TC-Region", m_region);

    // send http request
    auto response = http_client->MakeRequest(http_request, m_limiter.get(), m_limiter.get());
    if (response->GetResponseCode() != Aws::Http::HttpResponseCode::OK)
    {
        LOG_ERROR(log, "Failed to send request to Tencent Cloud STS AssumeRole with web identity creds provider.");
        return;
    }

    // parse http response
    auto body = Aws::String(Aws::IStreamBufIterator(response->GetResponseBody()), Aws::IStreamBufIterator());
    try
    {
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var result = parser.parse(body);
        const auto & obj = result.extract<Poco::JSON::Object::Ptr>();
        if (!obj->has("Response"))
        {
            LOG_ERROR(
                log,
                "Failed to parse response from Tencent Cloud STS AssumeRole with web identity creds provider.");
            return;
        }
        auto response_obj = obj->getObject("Response");
        if (response_obj->has("Error"))
        {
            auto error_obj = response_obj->getObject("Error");
            auto request_id = response_obj->optValue<Aws::String>("RequestId", "");
            LOG_ERROR(
                log,
                "Tencent Cloud STS AssumeRoleWithWebIdentity API failed, code={} message={} request_id={}.",
                error_obj->optValue<Aws::String>("Code", ""),
                error_obj->optValue<Aws::String>("Message", ""),
                request_id);
            return;
        }
        if (!response_obj->has("Credentials"))
        {
            LOG_ERROR(
                log,
                "Failed to parse response from Tencent Cloud STS AssumeRole with web identity creds provider.");
            return;
        }
        auto credentials = response_obj->getObject("Credentials");
        auto access_key_id = credentials->getValue<Aws::String>("TmpSecretId");
        auto secret_access_key = credentials->getValue<Aws::String>("TmpSecretKey");
        auto security_token = credentials->getValue<Aws::String>("Token");
        Aws::Utils::DateTime expiration_time;
        if (response_obj->has("ExpiredTime"))
        {
            const auto expiration_sec = response_obj->getValue<Int64>("ExpiredTime");
            expiration_time = Aws::Utils::DateTime(static_cast<int64_t>(expiration_sec) * 1000);
        }
        else if (response_obj->has("Expiration"))
        {
            expiration_time = Aws::Utils::DateTime(
                response_obj->getValue<Aws::String>("Expiration"),
                Aws::Utils::DateFormat::ISO_8601);
        }
        else
        {
            LOG_ERROR(log, "Failed to parse credential expiration time from Tencent Cloud STS response.");
            return;
        }

        LOG_TRACE(log, "Successfully retrieved credentials with AWS_ACCESS_KEY: {}", access_key_id);
        m_credentials = Aws::Auth::AWSCredentials(access_key_id, secret_access_key, security_token, expiration_time);
    }
    catch (const Poco::Exception & e)
    {
        LOG_ERROR(log, "Failed to parse response from Tencent Cloud STS, {}", e.displayText());
        return;
    }
}


bool TencentCloudSTSAssumeRoleWebIdentityCredentialsProvider::expiresSoon() const
{
    return (
        (m_credentials.GetExpiration() - Aws::Utils::DateTime::Now()).count()
        < CREDENTIAL_PROVIDER_EXPIRATION_GRACE_PERIOD);
}

void TencentCloudSTSAssumeRoleWebIdentityCredentialsProvider::refreshIfExpired()
{
    Aws::Utils::Threading::ReaderLockGuard guard(m_reloadLock);
    if (!m_credentials.IsEmpty() && !expiresSoon())
    {
        return;
    }

    guard.UpgradeToWriterLock();
    if (!m_credentials.IsExpiredOrEmpty() && !expiresSoon()) // double-checked lock to avoid refreshing twice
    {
        return;
    }

    Reload();
}

} // namespace DB::S3::TencentCloud
