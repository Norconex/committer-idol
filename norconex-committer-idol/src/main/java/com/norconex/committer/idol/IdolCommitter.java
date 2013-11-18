/* Copyright 2010-2013 Norconex Inc.
 *
 * This file is part of Norconex Idol Committer.
 *
 * Norconex Idol Committer is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Norconex Idol Committer is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Norconex Idol Committer. If not, see
 * <http://www.gnu.org/licenses/>.
 */
package com.norconex.committer.idol;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.norconex.committer.BaseCommitter;
import com.norconex.committer.CommitterException;
import com.norconex.commons.lang.config.IXMLConfigurable;
import com.norconex.commons.lang.map.Properties;

/**
 * Commits documents to Autonomy IDOL Server via a rest api.
 * <p>
 * XML configuration usage: Stephen was here today.....
 * </p>
 *
 * <pre>
 *   &lt;committer class="com.norconex.committer.idol.IdolCommitter"&gt;
 *      &lt;idolHost&gt;(Host to IDOL.)&lt;/idolHost&gt;
 *      &lt;idolPort&gt;(Port to IDOL.)&lt;/idolPort&gt;
 *      &lt;idolIndexPort&gt;(Port to IDOL Index.)&lt;/idolIndexPort&gt;
 *      &lt;idolDbName&gt;(IDOL Databse Name where to store documents.)&lt;/idolDbName&gt;
 *      &lt;dreAddDataParams&gt;
 *         &lt;param name="(parameter name)"&gt;(parameter value)&lt;/param&gt;
 *      &lt;/dreAddDataParams&gt;
 *      &lt;dreDeleteRefParams&gt;
 *         &lt;param name="(parameter name)"&gt;(parameter value)&lt;/param&gt;
 *      &lt;/dreDeleteRefParams&gt;
 *      &lt;referenceField keep="[false|true]"&gt;
 *         (Name of source field that will be mapped to the IDOL "DREREFERENCE" field.
 *         Default is the document reference metadata field:
 *         "document.reference". The metadata source field is
 *         deleted, unless "keep" is set to true.)
 *      &lt;/referenceField&gt;
 *      &lt;contentField keep="[false|true]"&gt;
 *         (If you wish to use a metadata field to act as the document
 *         "DRECONTENT" you can specify that field here. Default does not take a
 *         metadata field but rather the document content. Once re-mapped , the
 *         metadata source field is deleted, unless "keep" is set to  true.)
 *      &lt;/contentField&gt;
 *      &lt;queueDir&gt;(optional path where to queue files)&lt;/queueDir&gt;
 *      &lt;batchSize&gt;(queue size before sending to IDOL)&lt;/batchSize&gt;
 *      &lt;idolBatchSize&gt;
 *         (max number of docs to send IDOL at once. If not specified,
 *         the default is 100 [greater than one].)
 *      &lt;/idolBatchSize&gt;
 *   &lt;/committer&gt;
 * </pre>
 *
 * @author <a href="mailto:stephen.jacob@norconex.com">Stephen Jacob</a>
 */
@SuppressWarnings("restriction")
public class IdolCommitter extends BaseCommitter implements IXMLConfigurable {
    /*
     * DREADD Indexes content into IDOL server. DREADDDATA Indexes content over
     * socket into IDOL server. DREBACKUP Backs up IDOL server's Data index.
     * DRECHANGEMETA Changes documents' meta fields. DRECOMPACT Compacts IDOL
     * server's Data index. DRECREATEDBASE Creates an IDOL server database.
     * DREDELDBASE Deletes all documents from an IDOL server database.
     * DREDELETEDOC Deletes documents by ID. DREDELETEREF Deletes documents by
     * reference. DREEXPIRE Expires documents from IDOL server. DREEXPORTIDX
     * Exports IDX files from IDOL server. DREEXPORTREMOTE Exports XML files
     * from one IDOL server and indexes them into another. DREEXPORTXML Exports
     * XML files from IDOL server. DREFLUSHANDPAUSE Prepares IDOL server for a
     * snapshot (hot backup). DREINITIAL Resets IDOL server's Data index.
     * DREREMOVEDBASE Deletes an IDOL server database. DREREPLACE Changes
     * documents' field values. DRERESET Activates configuration-file changes.
     * DRERESIZEINDEXCACHE Dynamically resizes the index cache. DRESYNC Flushes
     * to disk the index cache. DREUNDELETEDOC Restores deleted documents.
     */

    /**
     * Making sure that if we read/write an object to file we we are using the
     * same version of the object and not new version with new incompatible
     * datatype.
     */
    private static final long serialVersionUID = 1;

    /**
     * Logging object to be uses to output debug/info/warning information.
     */
    private static final Logger LOG = LogManager.getLogger(IdolCommitter.class);

    /**
     * DREREFERENCE is the default key field in Autonomy Idol database.
     *
     */
    private static final String DEFAULT_IDOL_REF_FIELD = "DREREFERENCE";
    /**
     * DRECONTENT is the default field for content in Autonomy Idol Database.
     */
    private static final String DEFAULT_IDOL_CONTENT_FIELD = "DRECONTENT";

    private static final int DEFAULT_IDOL_BATCH_SIZE = 100;
    private static final int DEFAULT_IDOL_PORT = 9000;
    private static final int DEFAULT_IDOL_INDEX_PORT = 9001;
    private int idolBatchSize = DEFAULT_IDOL_BATCH_SIZE;
    private String idolDbName;
    private String idolHost;
    private int idolPort;
    private int idolIndexPort;

    /**
     * Getter for the variable IdolIndexPort
     *
     * @return idolIndexPort
     */
    public int getIdolIndexPort() {
        return idolIndexPort;
    }

    public void setIdolIndexPort(int idolIndexPort) {
        this.idolIndexPort = idolIndexPort;
    }

    private final List<QueuedAddedDocument> docsToAdd = new ArrayList<QueuedAddedDocument>();

    private final List<QueuedDeletedDocument> docsToRemove = new ArrayList<QueuedDeletedDocument>();

    private final Map<String, String> updateUrlParams = new HashMap<String, String>();

    private final Map<String, String> deleteUrlParams = new HashMap<String, String>();

    public int getIdolBatchSize() {
        return idolBatchSize;
    }

    public void setIdolBatchSize(int idolBatchSize) {
        this.idolBatchSize = idolBatchSize;
    }

    public String getIdolDbName() {
        return idolDbName;
    }

    public void setIdolDbName(String idolDbName) {
        this.idolDbName = idolDbName;
    }

    public String getIdolHost() {
        return idolHost;
    }

    public void setIdolHost(String idolHost) {
        this.idolHost = idolHost;
    }

    public int getIdolPort() {
        return idolPort;
    }

    public void setIdolPort(int idolPort) {
        this.idolPort = idolPort;
    }

    public List<QueuedAddedDocument> getDocsToAdd() {
        return docsToAdd;
    }

    public List<QueuedDeletedDocument> getDocsToRemove() {
        return docsToRemove;
    }

    public void setUpdateUrlParam(String name, String value) {
        updateUrlParams.put(name, value);
    }

    public void setDeleteUrlParam(String name, String value) {
        deleteUrlParams.put(name, value);
    }

    public String getUpdateUrlParam(String name) {
        return updateUrlParams.get(name);
    }

    public String getDeleteUrlParam(String name) {
        return deleteUrlParams.get(name);
    }

    public Set<String> getUpdateUrlParamNames() {
        return updateUrlParams.keySet();
    }

    public Set<String> getDeleteUrlParamNames() {
        return deleteUrlParams.keySet();
    }

    public Map<String, String> getUpdateUrlParams() {
        return updateUrlParams;
    }

    public Map<String, String> getDeleteUrlParams() {
        return deleteUrlParams;
    }

    // TODO check if http:// is already in the string
    public String getIdolUrl() {
        return "http://" + this.idolHost + ":" + this.idolPort + "/";
    }

    private String request(String url) {
        String idolResponse = null;
        HttpClient client = new DefaultHttpClient();
        HttpPost post = new HttpPost(url);
        // add header
        post.setHeader("User-Agent", "");

        List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();

        try {
            post.setEntity(new UrlEncodedFormEntity(urlParameters));
            HttpResponse response = client.execute(post);
            LOG.debug("\nSending 'POST' request to URL : " + url);
            LOG.debug("Post parameters : " + post.getEntity());
            LOG.debug("Response Code : "
                    + response.getStatusLine().getStatusCode());

            BufferedReader rd = new BufferedReader(new InputStreamReader(
                    response.getEntity().getContent(), "UTF-8"));

            StringBuffer result = new StringBuffer();
            String line = "";
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }
            idolResponse = result.toString();
            LOG.debug(result.toString());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return idolResponse;
    }

    public void commitToIdol() {
        this.request("http://" + this.getIdolHost() + ":"
                + this.getIdolIndexPort() + "/DRESYNC");
    }

    private String getDreReference(Properties prop) {
        String dreReferenceValue = "99";
        for (Entry<String, List<String>> entry : prop.entrySet()) {
            for (String value : entry.getValue()) {
                LOG.debug("value: " + value);
                if (entry.getKey().equals("document.reference")) {
                    dreReferenceValue = value;
                }
            }
        }

        return dreReferenceValue;
    }

    /**
     *
     * @param url
     * @param is
     * @param prop
     * @throws IOException
     */
    private void addToIdol(String url, InputStream is, Properties prop)
            throws IOException {
        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

        // add request header
        con.setRequestMethod("POST");
        con.setRequestProperty("User-Agent", "");
        con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");

        String idolDocument = "";
        // Create a database key for the idol idx document
        idolDocument = idolDocument.concat("\n#DREREFERENCE ")
                + this.getDreReference(prop);

        // Loop thru the list of properties and create idx fields accordingly.
        for (Entry<String, List<String>> entry : prop.entrySet()) {
            for (String value : entry.getValue()) {
                LOG.debug("value: " + value);
                idolDocument = idolDocument.concat("\n#DREFIELD "
                        + entry.getKey() + "=\"" + value + "\"");
            }
        }
        idolDocument = idolDocument.concat("\n#DREDBNAME "
                + this.getIdolDbName());
        idolDocument = idolDocument.concat("\n#DRECONTENT\n"
                + IOUtils.toString(is));
        idolDocument = idolDocument.concat("\n#DREENDDOC ");
        idolDocument = idolDocument.concat("\n#DREENDDATAREFERENCE");

        // Send post request
        con.setDoOutput(true);
        DataOutputStream wr = new DataOutputStream(con.getOutputStream());
        wr.writeBytes(idolDocument);
        wr.flush();
        wr.close();

        int responseCode = con.getResponseCode();
        LOG.debug("\nSending 'POST' request to URL : " + url);
        LOG.debug("Post parameters : " + idolDocument);
        LOG.debug("Response Code : " + responseCode);

        BufferedReader in = new BufferedReader(new InputStreamReader(
                con.getInputStream(), "UTF-8"));
        String inputLine = null;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        // print result
        LOG.debug(response.toString());
        // this.commitToIdol();

    }

    @Override
    protected void loadFromXml(XMLConfiguration xml) {
        setIdolHost(xml.getString("idolHost"));
        setIdolPort(xml.getInt("idolPort", DEFAULT_IDOL_PORT));
        setIdolIndexPort(xml.getInt("idolIndexPort", DEFAULT_IDOL_INDEX_PORT));
        setIdolBatchSize(xml.getInt("idolBatchSize", DEFAULT_IDOL_BATCH_SIZE));
        setBatchSize(xml.getInt("batchSize"));
        setIdolDbName(xml.getString("idolDbName"));
        LOG.debug("------" + xml.getString("idolDbName"));
        List<HierarchicalConfiguration> uparams = xml
                .configurationsAt("dreAddDataParams.param");
        for (HierarchicalConfiguration param : uparams) {
            setUpdateUrlParam(param.getString("[@name]"), param.getString(""));
        }

        List<HierarchicalConfiguration> dparams = xml
                .configurationsAt("dreDeleteRefParams.param");
        for (HierarchicalConfiguration param : dparams) {
            setDeleteUrlParam(param.getString("[@name]"), param.getString(""));
        }
    }

    @Override
    protected void commitAddedDocument(QueuedAddedDocument document)
            throws IOException {
        docsToAdd.add(document);
        if (docsToAdd.size() % idolBatchSize == 0) {
            persistToIdol();
        }
    }

    @Override
    protected void commitDeletedDocument(QueuedDeletedDocument document)
            throws IOException {
        docsToRemove.add(document);
        if (docsToRemove.size() % idolBatchSize == 0) {
            deleteFromIdol();
        }
    }

    // TODO need to refactor the url string into a method...
    private void persistToIdol() {
        LOG.info("Sending " + docsToAdd.size()
                + " documents to Idol for update.");
        String baseUrl = "http://" + this.getIdolHost() + ":"
                + this.getIdolIndexPort() + "/DREADDDATA?";

        for (QueuedAddedDocument qad : docsToAdd) {
            try {
                this.addToIdol(baseUrl, qad.getContentStream(),
                        qad.getMetadata());
                LOG.debug(qad.getMetadata());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        LOG.info("Done sending documents to Idol for update.");
    }

    private void deleteFromIdol() {
        LOG.info("Sending " + docsToRemove.size()
                + " documents to Idol for deletion.");

        try {
            for (QueuedDeletedDocument doc : docsToRemove) {
                doc.deleteFromQueue();
            }
            docsToRemove.clear();
        } catch (Exception e) {
            throw new CommitterException(
                    "Cannot delete document batch from Idol.", e);
        }
        LOG.info("Done sending documents to Idol for deletion.");
    }

    @Override
    protected void commitComplete() {
        if (!docsToAdd.isEmpty()) {
            persistToIdol();
        }
        if (!docsToRemove.isEmpty()) {
            deleteFromIdol();
        }
    }

    @Override
    protected void saveToXML(XMLStreamWriter writer) throws XMLStreamException {

    }

}
