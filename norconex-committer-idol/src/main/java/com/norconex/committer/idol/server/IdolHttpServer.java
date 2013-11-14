package com.norconex.committer.idol.server;


public class IdolHttpServer {
//	private static final Logger LOG = LogManager
//			.getLogger(IdolHttpServer.class);
//	private static final String USER_AGENT = null;
//	
//	private IdolResponse request(String url){
//		IdolResponse ir = new IdolResponse();
//		HttpClient client = new DefaultHttpClient();
//		HttpPost post = new HttpPost(url);
//		// add header
//		post.setHeader("User-Agent", USER_AGENT);
//
//		List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
//		
//		try {
//			post.setEntity(new UrlEncodedFormEntity(urlParameters));
//			HttpResponse response = client.execute(post);
//			LOG.debug("\nSending 'POST' request to URL : " + url);
//			LOG.debug("Post parameters : " + post.getEntity());
//			LOG.debug("Response Code : "
//					+ response.getStatusLine().getStatusCode());
//
//			BufferedReader rd = new BufferedReader(new InputStreamReader(response
//					.getEntity().getContent()));
//
//			StringBuffer result = new StringBuffer();
//			String line = "";
//			while ((line = rd.readLine()) != null) {
//				result.append(line);
//			}
//			ir.setResponse(result.toString());
//			LOG.debug(result.toString());
//		} catch (UnsupportedEncodingException e) {
//			e.printStackTrace();
//		} catch (ClientProtocolException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//
//		return ir;
//	}
//
//	public void createDataBase(String idolHost, int idolIndexPort, String idolDbName) {
//		String url = "http://"+idolHost+":"+String.valueOf(idolIndexPort)+"/DRECREATEDBASE?DREdbname="+idolDbName;
//		request(url);
//	}
//
//	public void deleteDataBase(String idolHost, int idolIndexPort,
//			String idolDbName) {
//		String url = "http://"+idolHost+":"+String.valueOf(idolIndexPort)+"/DREDELETEDBASE?DREdbname="+idolDbName;
//		request(url);
//		
//	}
}