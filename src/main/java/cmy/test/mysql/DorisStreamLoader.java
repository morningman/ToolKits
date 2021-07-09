// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package cmy.test.mysql;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;

public class DorisStreamLoader {
    // 1. 对于公有云公户，这里填写 Compute Node 地址以及 HTTP 协议访问端口（8040）。
    // 2. 对于开源用户，可以选择填写 FE 地址以及 FE 的 http_port，但须保证客户端和 BE 节点的连通性。
    private final static String HOST = "127.0.0.1";
    private final static int PORT = 9030;
    private final static String DATABASE = "db1";   // 要导入的数据库
    private final static String TABLE = "tbl1";     // 要导入的表
    private final static String USER = "root";      // Doris 用户名
    private final static String PASSWD = "12345";        // Doris 密码
    private final static String LOAD_FILE_NAME = "/Users/chenmingyu/Downloads/log/1.txt"; // 要导入的本地文件路径

    private final static String loadUrl = String.format("http://%s:%s/api/%s/%s/_stream_load",
            HOST, PORT, DATABASE, TABLE);

    private final static HttpClientBuilder httpClientBuilder2 = HttpClients
            .custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {

                @Override
                public boolean isRedirected(HttpRequest request, HttpResponse response, HttpContext context) {
                    return true;
                }

                public HttpUriRequest getRedirect(HttpRequest request, HttpResponse response, HttpContext context) throws ProtocolException {
                    URI uri = this.getLocationURI(request, response, context);
                    String method = request.getRequestLine().getMethod();
                    if (method.equalsIgnoreCase("HEAD")) {
                        return new HttpHead(uri);
                    } else if (method.equalsIgnoreCase("GET")) {
                        return new HttpGet(uri);
                    } else {
                        int status = response.getStatusLine().getStatusCode();
                        HttpUriRequest toReturn = null;
                        if (status == 307 || status == 308) {
                            toReturn = RequestBuilder.copy(request).setUri(uri).build();
                            toReturn.removeHeaders("Content-Length"); //Workaround for an apparent bug in HttpClient
                        } else {
                            toReturn = new HttpGet(uri);
                        }
                        return toReturn;
                    }
                }
            });

    private final static HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    // 如果连接目标是 FE，则需要处理 307 redirect。
                    return true;
                }
            });

    public void load(File file) throws Exception {
        try (CloseableHttpClient client = httpClientBuilder.build()) {
            HttpPut put = new HttpPut(loadUrl);
            put.setHeader(HttpHeaders.EXPECT, "100-continue");
            put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(USER, PASSWD));

            // 可以在 Header 中设置 stream load 相关属性，这里我们设置 label 和 column_separator。
            put.setHeader("label", "label1");
            put.setHeader("column_separator", ",");

            // 设置导入文件。
            // 这里也可以使用 StringEntity 来传输任意数据。
            // FileEntity entity = new FileEntity(file);
            String d = "1,2\n3,4";
            StringEntity entity = new StringEntity(d);
            put.setEntity(entity);

            try (CloseableHttpResponse response = client.execute(put)) {
                String loadResult = "";
                if (response.getEntity() != null) {
                    loadResult = EntityUtils.toString(response.getEntity());
                }

                final int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode != 200) {
                    throw new IOException(
                            String.format("Stream load failed. status: %s load result: %s", statusCode, loadResult));
                }

                System.out.println("Get load result: " + loadResult);
            }
        }
    }

    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    public static void main(String[] args) throws Exception {
        DorisStreamLoader loader = new DorisStreamLoader();
        File file = new File(LOAD_FILE_NAME);
        loader.load(file);
    }
}
