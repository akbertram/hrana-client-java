package com.bedatadriven.hrana;

import java.net.http.HttpClient;
import java.time.Duration;

/**
 * Thin wrapper around Java {@link java.net.http.HttpClient} that knows how to
 * create Hrana HTTP streams. It prepares the base pipeline endpoint URL for
 * Hrana v3 over HTTP with Protobuf encoding and carries the optional JWT that
 * is attached to requests as a Bearer token.
 *
 * <p>Use {@link #newStream()} to obtain a new {@link HranaHttpStream} which
 * represents a single logical Hrana stream (similar to a connection). Each
 * stream is stateful and keeps a baton provided by the server between
 * requests, as specified in SPEC.md.</p>
 */
public class HranaHttpClient {

  private final HttpClient httpClient;
  private String streamUrl;
  private final String jwt;

  /**
   * Creates a client bound to a Hrana HTTP base URL.
   *
   * <p>The final pipeline endpoint used is derived from {@code baseUrl} by
   * trimming a trailing slash (if any) and appending {@code /v3-protobuf/pipeline}.
   * Requests are sent with HTTP/2 when available and a 10 second connect timeout.</p>
   *
   * @param baseUrl Base URL of the Hrana server (e.g. https://example.com).
   * @param jwt Optional bearer token; if non-empty it will be sent as
   *            {@code Authorization: Bearer <jwt>} with each request.
   */
  public HranaHttpClient(String baseUrl, String jwt) {
    this.streamUrl = baseUrl.replaceAll("/$", "") + "/v3-protobuf/pipeline";
    this.jwt = jwt;
    this.httpClient = HttpClient.newBuilder()
      .version(HttpClient.Version.HTTP_2)
      .connectTimeout(Duration.ofSeconds(10))
      .build();
  }

  /**
   * Creates a new Hrana HTTP stream bound to this client's base URL and JWT.
   *
   * <p>Each stream corresponds to a single stateful sequence of requests. The
   * server issues a baton on first use which the stream will automatically
   * attach to subsequent requests.</p>
   *
   * @return a new {@link HranaHttpStream} ready to execute statements.
   */
  public HranaHttpStream newStream() {
    return new HranaHttpStream(streamUrl, jwt, httpClient);
  }

}