package com.norconex.committer.idol.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
DREADD Indexes content into IDOL server.
DREADDDATA Indexes content over socket into IDOL server.
DREBACKUP Backs up IDOL server's Data index.
DRECHANGEMETA Changes documents' meta fields.
DRECOMPACT Compacts IDOL server's Data index.
DRECREATEDBASE Creates an IDOL server database.
DREDELDBASE Deletes all documents from an IDOL server database.
DREDELETEDOC Deletes documents by ID.
DREDELETEREF Deletes documents by reference.
DREEXPIRE Expires documents from IDOL server.
DREEXPORTIDX Exports IDX files from IDOL server.
DREEXPORTREMOTE Exports XML files from one IDOL server and indexes them into another.
DREEXPORTXML Exports XML files from IDOL server.
DREFLUSHANDPAUSE Prepares IDOL server for a snapshot (hot backup).
DREINITIAL Resets IDOL server's Data index.
DREREMOVEDBASE Deletes an IDOL server database.
DREREPLACE Changes documents' field values.
DRERESET Activates configuration-file changes.
DRERESIZEINDEXCACHE Dynamically resizes the index cache.
DRESYNC Flushes to disk the index cache.
DREUNDELETEDOC Restores deleted documents.
 * @param host the ip of autonomy
 * @param indexPort the port for create, update and delete
 * @param queryPort the port for find
 */

public class IdolHttpServer {
	private static final Logger LOG = LogManager
			.getLogger(IdolHttpServer.class);
	private static final String USER_AGENT = null;
	
	private IdolResponse request(String url){
		IdolResponse ir = new IdolResponse();
		HttpClient client = new DefaultHttpClient();
		HttpPost post = new HttpPost(url);
		// add header
		post.setHeader("User-Agent", USER_AGENT);

		List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
		
		try {
			post.setEntity(new UrlEncodedFormEntity(urlParameters));
			HttpResponse response = client.execute(post);
			LOG.debug("\nSending 'POST' request to URL : " + url);
			LOG.debug("Post parameters : " + post.getEntity());
			LOG.debug("Response Code : "
					+ response.getStatusLine().getStatusCode());

			BufferedReader rd = new BufferedReader(new InputStreamReader(response
					.getEntity().getContent()));

			StringBuffer result = new StringBuffer();
			String line = "";
			while ((line = rd.readLine()) != null) {
				result.append(line);
			}
			ir.setResponse(result.toString());
			LOG.debug(result.toString());
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return ir;
	}

	public void createDataBase(String idolHost, int idolIndexPort, String idolDbName) {
		String url = "http://"+idolHost+":"+String.valueOf(idolIndexPort)+"/DRECREATEDBASE?DREdbname="+idolDbName;
		request(url);
	}

	public void deleteDataBase(String idolHost, int idolIndexPort,
			String idolDbName) {
		String url = "http://"+idolHost+":"+String.valueOf(idolIndexPort)+"/DREDELETEDBASE?DREdbname="+idolDbName;
		request(url);
		
	}
}