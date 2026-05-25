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

#pragma once

#include <Common/Logger.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/http/HttpClientFactory.h>
#include <common/types.h>

namespace DB::S3::TencentCloud
{
//  AWSCredentialsProvider for Tencent Cloud using OIDC
class TencentCloudSTSAssumeRoleWebIdentityCredentialsProvider : public Aws::Auth::AWSCredentialsProvider
{
public:
    static std::shared_ptr<Aws::Auth::AWSCredentialsProvider> build(const Aws::Client::ClientConfiguration & cfg);

    TencentCloudSTSAssumeRoleWebIdentityCredentialsProvider(
        const Aws::Client::ClientConfiguration & cfg,
        const String & region_id,
        const String & role_arn,
        const String & provider_id,
        const String & token_file);

    /**
     * Retrieves the credentials if found, otherwise returns empty credential set.
     */
    Aws::Auth::AWSCredentials GetAWSCredentials() override;

protected:
    void Reload() override;

private:
    void refreshIfExpired();
    Aws::String calculateRequestBody() const;
    bool expiresSoon() const;

private:
    Aws::Auth::AWSCredentials m_credentials;
    Aws::Client::ClientConfiguration m_cfg;
    Aws::String m_region;
    Aws::String m_role_arn;
    Aws::String m_provider_id;
    Aws::String m_token_file;
    Aws::String m_session_name;
    Aws::String m_token;
    Aws::String m_endpoint;
    bool m_initialized;
    std::shared_ptr<Aws::Http::HttpClientFactory> m_http_client_factory;
    std::shared_ptr<Aws::Utils::RateLimits::RateLimiterInterface> m_limiter;
    LoggerPtr log;
};

} // namespace DB::S3::TencentCloud
