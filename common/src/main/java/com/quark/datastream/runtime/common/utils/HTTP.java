package com.quark.datastream.runtime.common.utils;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

public class HTTP {

	private HttpClient client = null;
	private HttpHost proxy = null;
	private URIBuilder uriBuilder = null;

	private boolean initialized = false;

	private JsonParser jsonParser = null;

	public HTTP() {
	}

	/**
	 * Returns an HTTP object that initiates the given connection. The address
	 * argument must provide destination in the form of IP/HOSTNAME:PORT. The
	 * scheme argument must specify protocol to use, i.e., http or https.
	 *
	 * @param address
	 *            Address of the destination (or server)
	 * @param scheme
	 *            e.g.: http, https
	 * @return An object that provides RESTful interfaces for the given
	 *         destination.
	 */
	public HTTP initialize(String address, String scheme) {
		String ip = address.substring(0, address.indexOf(":"));
		int port = Integer.parseInt(address.substring(address.indexOf(":") + 1, address.length()));

		return initialize(ip, port, scheme);
	}

	/**
	 * Initialize HTTP.
	 *
	 * @param host
	 *            hostname
	 * @param port
	 *            port number
	 * @param scheme
	 *            e.g.: http, https
	 */
	public HTTP initialize(String host, int port, String scheme) {
		if (host == null || port <= 0) {
			throw new RuntimeException("Invalid host or port entered.");
		}

		host = host.trim();
		if (host.isEmpty()) {
			throw new RuntimeException("Invalid host or port entered.");
		}

		this.client = HttpClients.createDefault();

		this.jsonParser = new JsonParser();

		this.uriBuilder = new URIBuilder();
		this.uriBuilder.setScheme(scheme).setHost(host).setPort(port);

		this.initialized = true;

		return this;
	}

	public void setProxy(String host, int port, String scheme) {
		this.proxy = new HttpHost(host, port, scheme);
	}

	private boolean isProxyAvailable() {
		return this.proxy != null;
	}

	private URI createUri(String path, Map<String, String> args) throws URISyntaxException {
		this.uriBuilder.clearParameters();
		this.uriBuilder.setPath(path);

		if (args != null && args.size() > 0) {
			for (Map.Entry<String, String> entry : args.entrySet()) {
				this.uriBuilder.addParameter(entry.getKey(), entry.getValue());
			}
		}

		return this.uriBuilder.build();
	}

	private HttpResponse executeRequest(HttpRequestBase request) throws IOException {
		HttpResponse response;

		if (isProxyAvailable()) {
			response = getClient().execute(this.proxy, request);
		} else {
			response = getClient().execute(request);
		}
		return response;
	}

	public HttpClient getClient() {
		return this.client;
	}

	public JsonElement get(String path) throws Exception {
		return get(path, null);
	}

	/**
	 * Returns an JsonElement object as the result of GET with arguments. The
	 * path argument must specify path to the REST server, and the args argument
	 * must describe the key-value sets of arguments for a GET request.
	 *
	 * @param path
	 *            REST server path from /
	 * @param args
	 *            Arguments of a GET request
	 * @return Results of the GET request with arguments
	 * @throws Exception
	 *             if connection is bad, or failed.
	 */
	public JsonElement get(String path, Map<String, String> args) throws Exception {
		throwExceptionIfNotInitialized();

		HttpGet request = null;
		try {
			URI uri = createUri(path, args);
			request = new HttpGet(uri);
			HttpResponse response = executeRequest(request);

			int httpStatusCode = response.getStatusLine().getStatusCode();
			if (httpStatusCode != HttpStatus.SC_OK && httpStatusCode != HttpStatus.SC_ACCEPTED) {
				throw new HttpResponseException(httpStatusCode, String.format("Bad HTTP status: %d", httpStatusCode));
			}

			String rawJson = EntityUtils.toString(response.getEntity());

			return this.jsonParser.parse(rawJson);

		} finally {
			if (request != null) {
				request.releaseConnection();
			}
		}
	}

	/**
	 * Returns an JsonElement object as the result of DELETE. The path argument
	 * must specify path to the REST server.
	 *
	 * @param path
	 *            REST server path from /
	 * @return Results of the DELETE request with arguments
	 * @throws Exception
	 *             if connection is bad, or failed.
	 */
	public JsonElement delete(String path) throws Exception {
		throwExceptionIfNotInitialized();

		HttpDelete request = null;
		try {
			URI uri = createUri(path, null);
			request = new HttpDelete(uri);
			HttpResponse response = executeRequest(request);

			int httpStatusCode = response.getStatusLine().getStatusCode();
			if (httpStatusCode != HttpStatus.SC_OK) {
				throw new HttpResponseException(httpStatusCode, String.format("Bad HTTP status: %d", httpStatusCode));
			}

			String rawJson = EntityUtils.toString(response.getEntity());

			return this.jsonParser.parse(rawJson);

		} finally {
			if (request != null) {
				request.releaseConnection();
			}
		}
	}

	/**
	 * Returns an JsonElement object as the result of POST with a file. The path
	 * argument must specify path to the REST server, and the fileToUpload must
	 * provide the file path to attach on this POST request.
	 *
	 * @param path
	 *            REST server path from /
	 * @param fileToUpload
	 *            the file to attach on the request
	 * @return Results of the POST request with the file
	 * @throws Exception
	 *             if connection is bad, or failed.
	 */
	public JsonElement post(String path, File fileToUpload) throws Exception {
		throwExceptionIfNotInitialized();
		HttpPost request = null;
		try {
			URI uri = createUri(path, null);
			request = new HttpPost(uri);
			// request.addHeader(HttpHeaders.CONTENT_TYPE,
			// "application/x-java-archive");

			MultipartEntityBuilder builder = MultipartEntityBuilder.create();
			builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
			builder.addBinaryBody("file", fileToUpload);

			request.setEntity(builder.build());

			HttpResponse response = executeRequest(request);

			int httpStatusCode = response.getStatusLine().getStatusCode();
			if (httpStatusCode != HttpStatus.SC_OK) {
				throw new HttpResponseException(httpStatusCode, String.format("Bad HTTP status: %d", httpStatusCode));
			}

			String rawJson = EntityUtils.toString(response.getEntity());

			return this.jsonParser.parse(rawJson);

		} finally {
			if (request != null) {
				request.releaseConnection();
			}
		}
	}

	public JsonElement post(String path, Map<String, String> args) throws Exception {
		return post(path, args, false);
	}

	/**
	 * Returns an JsonElement object as the result of POST with arguments. The
	 * path argument must specify path to the REST server, and the args must
	 * provide the key-value sets of arguments for a POST request.
	 *
	 * @param path
	 *            REST server path from /
	 * @param args
	 *            Arguments of a POST request
	 * @param useArgAsParam
	 *            Specification to deliver the arguments as parameters on the
	 *            request
	 * @return Results of the POST request with the arguments
	 * @throws Exception
	 *             if connection is bad, or failed.
	 */
	public JsonElement post(String path, Map<String, String> args, boolean useArgAsParam) throws Exception {
		throwExceptionIfNotInitialized();

		HttpPost request = null;
		try {
			URI uri;
			if (useArgAsParam) {
				uri = createUri(path, args);
			} else {
				uri = createUri(path, null);
			}

			request = new HttpPost(uri);

			if (!useArgAsParam) {
				List<NameValuePair> entity = new ArrayList<>();
				if (args != null && args.size() > 0) {
					for (Map.Entry<String, String> arg : args.entrySet()) {
						entity.add(new BasicNameValuePair(arg.getKey(), arg.getValue()));
					}
				}
				request.setEntity(new UrlEncodedFormEntity(entity));
			}

			HttpResponse response = executeRequest(request);

			int httpStatusCode = response.getStatusLine().getStatusCode();
			if (httpStatusCode != HttpStatus.SC_OK) {
				throw new HttpResponseException(httpStatusCode, String.format("Bad HTTP status: %d", httpStatusCode));
			}

			String rawJson = EntityUtils.toString(response.getEntity());

			return this.jsonParser.parse(rawJson);

		} finally {
			if (request != null) {
				request.releaseConnection();
			}
		}
	}

	/**
	 * Returns an JsonElement object as the result of POST with encoded String
	 * value. The path argument must specify path to the REST server, and the
	 * dataString must provide String value to deliver.
	 *
	 * @param path
	 *            REST server path from /
	 * @param dataString
	 *            Application URL-encoded String value
	 * @return Results of the POST request with String value
	 * @throws Exception
	 *             if connection is bad, or failed.
	 */
	public JsonElement post(String path, String dataString) throws Exception {
		throwExceptionIfNotInitialized();
		HttpPost request = null;
		try {
			URI uri = createUri(path, null);
			request = new HttpPost(uri);
			StringEntity entity = new StringEntity(dataString, ContentType.APPLICATION_FORM_URLENCODED);
			request.setEntity(entity);

			HttpResponse response = executeRequest(request);

			int httpStatusCode = response.getStatusLine().getStatusCode();
			if (httpStatusCode != HttpStatus.SC_OK) {
				String errorMsg = String.format("Bad Connection.HTTP status: %d", httpStatusCode);
				throw new HttpResponseException(httpStatusCode, errorMsg);
			}

			String rawJson = EntityUtils.toString(response.getEntity());
			return this.jsonParser.parse(rawJson);
		} finally {
			if (request != null) {
				request.releaseConnection();
			}
		}
	}

	/**
	 * Returns an JsonElement object as the result of PATCH with encoded String
	 * value. The path argument must specify path to the REST server, and the
	 * dataString must provide String value to deliver.
	 *
	 * @param path
	 *            REST server path from /
	 * @param dataString
	 *            Application URL-encoded String value
	 * @return Results of the PATCH request with String value
	 * @throws Exception
	 *             if connection is bad, or failed.
	 */
	public JsonElement patch(String path, String dataString) throws Exception {
		throwExceptionIfNotInitialized();
		HttpPatch request = null;
		try {
			URI uri = createUri(path, null);
			request = new HttpPatch(uri);
			StringEntity entity = new StringEntity(dataString, ContentType.APPLICATION_FORM_URLENCODED);
			request.setEntity(entity);

			HttpResponse response = executeRequest(request);

			int httpStatusCode = response.getStatusLine().getStatusCode();
			if (httpStatusCode != HttpStatus.SC_OK) {
				String errorMsg = String.format("Bad Connection.HTTP status: %d", httpStatusCode);
				throw new HttpResponseException(httpStatusCode, errorMsg);
			}

			String rawJson = EntityUtils.toString(response.getEntity());
			return this.jsonParser.parse(rawJson);
		} finally {
			if (request != null) {
				request.releaseConnection();
			}
		}

	}

	private void throwExceptionIfNotInitialized() {
		if (!this.initialized) {
			throw new RuntimeException(HTTP.class.getSimpleName() + " is not initialized.");
		}
	}

}
