/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.cli;

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.SocketChannelSocketFactory;
import com.facebook.presto.client.StatementClient;
import com.google.common.net.HostAndPort;
import okhttp3.OkHttpClient;

import java.io.Closeable;
import java.io.File;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.facebook.presto.client.ClientSession.stripTransactionId;
import static com.facebook.presto.client.GCSOAuthInterceptor.GCS_CREDENTIALS_PATH_KEY;
import static com.facebook.presto.client.GCSOAuthInterceptor.GCS_OAUTH_SCOPES_KEY;
import static com.facebook.presto.client.OkHttpUtil.basicAuth;
import static com.facebook.presto.client.OkHttpUtil.setupCookieJar;
import static com.facebook.presto.client.OkHttpUtil.setupGCSOauth;
import static com.facebook.presto.client.OkHttpUtil.setupHttpProxy;
import static com.facebook.presto.client.OkHttpUtil.setupKerberos;
import static com.facebook.presto.client.OkHttpUtil.setupSocksProxy;
import static com.facebook.presto.client.OkHttpUtil.setupSsl;
import static com.facebook.presto.client.OkHttpUtil.setupTimeouts;
import static com.facebook.presto.client.OkHttpUtil.tokenAuth;
import static com.facebook.presto.client.StatementClientFactory.newStatementClient;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * 查询运行器，
 * 主要是构建了OkHttpClient，以便后序向Coordinate发送请求
 */
public class QueryRunner
        implements Closeable
{
    private final AtomicReference<ClientSession> session;
    private final boolean debug;
    private final OkHttpClient httpClient;
    private final Consumer<OkHttpClient.Builder> sslSetup;

    /**
     * 构建 OkHttpClient
     */
    public QueryRunner(
            ClientSession session,
            boolean debug,
            Optional<HostAndPort> socksProxy,
            Optional<HostAndPort> httpProxy,
            Optional<String> keystorePath,
            Optional<String> keystorePassword,
            Optional<String> truststorePath,
            Optional<String> truststorePassword,
            Optional<String> accessToken,
            Optional<String> user,
            Optional<String> password,
            Optional<String> kerberosPrincipal,
            Optional<String> kerberosRemoteServiceName,
            Optional<String> kerberosConfigPath,
            Optional<String> kerberosKeytabPath,
            Optional<String> kerberosCredentialCachePath,
            boolean kerberosUseCanonicalHostname)
    {
        this.session = new AtomicReference<>(requireNonNull(session, "session is null"));
        this.debug = debug;

        // 设置ssl
        this.sslSetup = builder -> setupSsl(builder, keystorePath, keystorePassword, truststorePath, truststorePassword);

        OkHttpClient.Builder builder = new OkHttpClient.Builder();

        // 创建SocketChannelSocketFactory
        builder.socketFactory(new SocketChannelSocketFactory());

        // 构建okhttp相关条件
        setupTimeouts(builder, 30, SECONDS); // 设置超时时间
        setupCookieJar(builder); //
        setupSocksProxy(builder, socksProxy);
        setupHttpProxy(builder, httpProxy);
        setupBasicAuth(builder, session, user, password);
        setupTokenAuth(builder, session, accessToken);

        if (kerberosRemoteServiceName.isPresent()) {
            checkArgument(session.getServer().getScheme().equalsIgnoreCase("https"),
                    "Authentication using Kerberos requires HTTPS to be enabled");
            setupKerberos(
                    builder,
                    kerberosRemoteServiceName.get(),
                    kerberosUseCanonicalHostname,
                    kerberosPrincipal,
                    kerberosConfigPath.map(File::new),
                    kerberosKeytabPath.map(File::new),
                    kerberosCredentialCachePath.map(File::new));
        }

        Optional.ofNullable(session.getExtraCredentials().get(GCS_CREDENTIALS_PATH_KEY))
                .ifPresent(credentialPath -> setupGCSOauth(builder, credentialPath, Optional.ofNullable(session.getExtraCredentials().get(GCS_OAUTH_SCOPES_KEY))));

        this.httpClient = builder.build();
    }

    public ClientSession getSession()
    {
        return session.get();
    }

    public void setSession(ClientSession session)
    {
        this.session.set(requireNonNull(session, "session is null"));
    }

    public boolean isDebug()
    {
        return debug;
    }

    public Query startQuery(String query)
    {
        return new Query(startInternalQuery(session.get(), query), debug);
    }

    public StatementClient startInternalQuery(String query)
    {
        return startInternalQuery(stripTransactionId(session.get()), query);
    }

    /**
     * 创建一个客户端
     * @param session
     * @param query
     * @return
     */
    private StatementClient startInternalQuery(ClientSession session, String query)
    {
        // 构建客户端
        OkHttpClient.Builder builder = httpClient.newBuilder();
        sslSetup.accept(builder);
        OkHttpClient client = builder.build();

        // 创建StatementClientV1
        return newStatementClient(client, session, query);
    }

    @Override
    public void close()
    {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }

    private static void setupBasicAuth(
            OkHttpClient.Builder clientBuilder,
            ClientSession session,
            Optional<String> user,
            Optional<String> password)
    {
        if (user.isPresent() && password.isPresent()) {
            checkArgument(session.getServer().getScheme().equalsIgnoreCase("https"),
                    "Authentication using username/password requires HTTPS to be enabled");
            clientBuilder.addInterceptor(basicAuth(user.get(), password.get()));
        }
    }

    private static void setupTokenAuth(
            OkHttpClient.Builder clientBuilder,
            ClientSession session,
            Optional<String> accessToken)
    {
        if (accessToken.isPresent()) {
            checkArgument(session.getServer().getScheme().equalsIgnoreCase("https"),
                    "Authentication using an access token requires HTTPS to be enabled");
            clientBuilder.addInterceptor(tokenAuth(accessToken.get()));
        }
    }
}
