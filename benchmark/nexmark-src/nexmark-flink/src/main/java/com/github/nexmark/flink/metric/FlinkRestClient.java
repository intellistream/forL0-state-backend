/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.nexmark.flink.metric;

import com.github.nexmark.flink.metric.tps.TpsMetric;
import com.github.nexmark.flink.utils.NexmarkUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A HTTP client to request TPS metric to JobMaster REST API.
 */
public class FlinkRestClient {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkRestClient.class);

	private static int CONNECT_TIMEOUT = 5000;
	private static int SOCKET_TIMEOUT = 60000;
	private static int CONNECTION_REQUEST_TIMEOUT = 10000;
	private static int MAX_IDLE_TIME = 60000;
	private static int MAX_CONN_TOTAL = 60;
	private static int MAX_CONN_PER_ROUTE = 30;

	private final String jmEndpoint;
	private final CloseableHttpClient httpClient;
	private final Map<String, String> jobIds;
	private volatile String lastJobId;

	public FlinkRestClient(String jmAddress, int jmPort) {
		this.jmEndpoint = jmAddress + ":" + jmPort;

		RequestConfig requestConfig = RequestConfig.custom()
			.setSocketTimeout(SOCKET_TIMEOUT)
			.setConnectTimeout(CONNECT_TIMEOUT)
			.setConnectionRequestTimeout(CONNECTION_REQUEST_TIMEOUT)
			.build();
		PoolingHttpClientConnectionManager httpClientConnectionManager = new PoolingHttpClientConnectionManager();
		httpClientConnectionManager.setValidateAfterInactivity(MAX_IDLE_TIME);
		httpClientConnectionManager.setDefaultMaxPerRoute(MAX_CONN_PER_ROUTE);
		httpClientConnectionManager.setMaxTotal(MAX_CONN_TOTAL);

		this.httpClient = HttpClientBuilder.create()
			.setConnectionManager(httpClientConnectionManager)
			.setDefaultRequestConfig(requestConfig)
			.build();

		this.jobIds = new ConcurrentHashMap<>(50);
		this.lastJobId = "";
	}

	public synchronized void updateAllJobStatus() {
		String url = String.format("http://%s/jobs/overview", jmEndpoint);
		String response = executeAsString(url);
		try {
			jobIds.clear();
			lastJobId = "";
			JsonNode jsonNode = NexmarkUtils.MAPPER.readTree(response);
			JsonNode jobs = jsonNode.get("jobs");
			long latestStartTime = Long.MIN_VALUE;
			long latestModificationTime = Long.MIN_VALUE;
			String latestRunningJobId = "";
			String latestSeenJobId = "";
			for (JsonNode job : jobs) {
				String id = job.get("jid").asText();
				String state = job.get("state").asText();
				jobIds.put(id, state);

				long startTime = job.has("start-time") ? job.get("start-time").asLong(Long.MIN_VALUE) : Long.MIN_VALUE;
				long modificationTime = job.has("last-modification") ? job.get("last-modification").asLong(Long.MIN_VALUE) : Long.MIN_VALUE;

				if (state.equalsIgnoreCase("RUNNING") || state.equalsIgnoreCase("INITIALIZING") || state.equalsIgnoreCase("CREATED") || state.equalsIgnoreCase("RECONCILING")) {
					if (startTime >= latestStartTime || modificationTime >= latestModificationTime) {
						latestRunningJobId = id;
						latestStartTime = startTime;
						latestModificationTime = modificationTime;
					}
				}

				if (startTime >= latestStartTime || modificationTime >= latestModificationTime) {
					latestSeenJobId = id;
					latestStartTime = startTime;
					latestModificationTime = modificationTime;
				}
			}

			lastJobId = !isNullOrEmpty(latestRunningJobId) ? latestRunningJobId : latestSeenJobId;
		} catch (JsonProcessingException e) {
			throw new RuntimeException("The response is not a valid JSON string:\n" + response, e);
		}
	}

	public void cancelJob(String jobId) {
		LOG.info("Stopping Job: {}", jobId);
		String url = String.format("http://%s/jobs/%s?mode=cancel", jmEndpoint, jobId);
		patch(url);
	}

	public String getCurrentJobId() {
		updateAllJobStatus();
		return lastJobId;
	}

	public boolean isJobRunning() {
		updateAllJobStatus();
		return !isNullOrEmpty(lastJobId) && jobIds.get(lastJobId).equalsIgnoreCase("RUNNING");
	}

	public boolean isJobCancellingOrFinished() {
		updateAllJobStatus();
		if (!isNullOrEmpty(lastJobId)) {
			String status = jobIds.get(lastJobId);
			return status.equalsIgnoreCase("CANCELLING") || status.equalsIgnoreCase("CANCELED") || status.equalsIgnoreCase("FINISHED");
		}
		return true;
	}

	private static boolean isNullOrEmpty(String string) {
		return string == null || string.length() == 0;
	}

	public String getSourceVertexId(String jobId) {
		return getMetricVertexId(jobId, "source");
	}

	public String getMetricVertexId(String jobId, String targetVertex) {
		String url = String.format("http://%s/jobs/%s", jmEndpoint, jobId);
		String response = executeAsString(url);
		try {
			JsonNode jsonNode = NexmarkUtils.MAPPER.readTree(response);
			JsonNode vertices = jsonNode.get("vertices");
			String target = targetVertex == null ? "sink" : targetVertex.trim().toLowerCase();
			if ("source".equals(target)) {
				for (JsonNode vertex : vertices) {
					String name = vertex.get("name").asText();
					if (name.startsWith("Source:")) {
						return vertex.get("id").asText();
					}
				}
				throw new IllegalArgumentException("Can't find source vertex from response:\n" + response);
			}
			if ("sink".equals(target)) {
				for (int i = vertices.size() - 1; i >= 0; i--) {
					JsonNode vertex = vertices.get(i);
					String name = vertex.get("name").asText();
					if (name.startsWith("Sink:") || name.contains("Sink")) {
						return vertex.get("id").asText();
					}
				}
				JsonNode lastVertex = vertices.get(vertices.size() - 1);
				checkArgument(
					!lastVertex.get("name").asText().startsWith("Source:"),
					"The last vertex is a source; can't infer sink vertex.");
				return lastVertex.get("id").asText();
			}
			throw new IllegalArgumentException("Unsupported TPS metric vertex target: " + targetVertex);
		} catch (Exception e) {
			throw new RuntimeException("The response is not a valid JSON string:\n" + response, e);
		}
	}

	public String getTpsMetricName(String jobId, String vertexId) {
		return getTpsMetricName(jobId, vertexId, "source");
	}

	public String getTpsMetricName(String jobId, String vertexId, String targetVertex) {
		String url = String.format("http://%s/jobs/%s/vertices/%s/subtasks/metrics", jmEndpoint, jobId, vertexId);
		String response = executeAsString(url);
		try {
			String target = targetVertex == null ? "sink" : targetVertex.trim().toLowerCase();
			String metricSuffix = "source".equals(target) ? ".numRecordsOutPerSecond" : ".numRecordsInPerSecond";
			String metricNameExact = "source".equals(target) ? "numRecordsOutPerSecond" : "numRecordsInPerSecond";
			ArrayNode arrayNode = (ArrayNode) NexmarkUtils.MAPPER.readTree(response);
			for (JsonNode node : arrayNode) {
				String metricName = node.get("id").asText();
				if (metricName.endsWith(metricSuffix) || metricName.equals(metricNameExact)) {
					return metricName;
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("The response is not a valid JSON string:\n" + response, e);
		}
		throw new RuntimeException("Can't find TPS metric name from the response:\n" + response);
	}

	public synchronized TpsMetric getTpsMetric(String jobId, String vertexId, String tpsMetricName) {
		String url = String.format(
			"http://%s/jobs/%s/vertices/%s/subtasks/metrics?get=%s",
			jmEndpoint,
			jobId,
			vertexId,
			tpsMetricName);
		String response = executeAsString(url);
		return TpsMetric.fromJson(response);
	}

	private void patch(String url) {
		HttpPatch httpPatch = new HttpPatch();
		httpPatch.setURI(URI.create(url));
		HttpResponse response;
		try {
			httpPatch.setHeader("Connection", "close");
			response = httpClient.execute(httpPatch);
			int httpCode = response.getStatusLine().getStatusCode();
			if (httpCode == HttpStatus.SC_CONFLICT) {
				return;
			}
			if (httpCode != HttpStatus.SC_ACCEPTED) {
				String msg = String.format("http execute failed,status code is %d", httpCode);
				throw new RuntimeException(msg);
			}
		} catch (Exception e) {
			httpPatch.abort();
			throw new RuntimeException(e);
		} finally {
			httpPatch.releaseConnection();
		}
	}

	private String executeAsString(String url) {
		HttpGet httpGet = new HttpGet();
		httpGet.setURI(URI.create(url));
		try {
			HttpEntity entity = execute(httpGet).getEntity();
			if (entity != null) {
				return EntityUtils.toString(entity, Consts.UTF_8);
			}
		} catch (Exception e) {
			throw new RuntimeException("Failed to request URL " + url, e);
		} finally {
			httpGet.releaseConnection();
		}
		throw new RuntimeException(String.format("Response of URL %s is null.", url));
	}

	private HttpResponse execute(HttpRequestBase httpRequestBase) throws Exception {
		HttpResponse response;
		try {
			httpRequestBase.setHeader("Connection", "close");
			response = httpClient.execute(httpRequestBase);
			int httpCode = response.getStatusLine().getStatusCode();
			if (httpCode != HttpStatus.SC_OK) {
				String msg = String.format("http execute failed,status code is %d", httpCode);
				throw new RuntimeException(msg);
			}
			return response;
		} catch (Exception e) {
			httpRequestBase.abort();
			throw e;
		}
	}

	public synchronized void close() {
		try {
			if (httpClient != null) {
				httpClient.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
