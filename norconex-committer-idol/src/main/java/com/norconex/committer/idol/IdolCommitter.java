/* Copyright 2010-2013 Norconex Inc.
 *
 * This file is part of Norconex Committer IDOL.
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
 * along with Norconex Committer IDOL. If not, see
 * <http://www.gnu.org/licenses/>.
 */
package com.norconex.committer.idol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
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
import org.apache.commons.lang3.CharEncoding;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.norconex.committer.AbstractMappedCommitter;
import com.norconex.committer.CommitterException;
import com.norconex.committer.IAddOperation;
import com.norconex.committer.ICommitOperation;
import com.norconex.committer.IDeleteOperation;
import com.norconex.commons.lang.EqualsUtil;
import com.norconex.commons.lang.map.Properties;
import com.norconex.commons.lang.url.QueryString;

/**
 * Commits documents to Autonomy IDOL Server via a rest api.
 * <p>
 * XML configuration usage:
 * </p>
 *
 * <pre>
 *   &lt;committer class="com.norconex.committer.idol.IdolCommitter"&gt;
 *      &lt;host&gt;(Host to IDOL.)&lt;/host&gt;
 *      &lt;aciPort&gt;(Port to IDOL.)&lt;/aciPort&gt;
 *      &lt;indexPort&gt;(Port to IDOL Index.)&lt;/indexPort&gt;
 *      &lt;databaseName&gt;(IDOL Databse Name where to store documents.)&lt;/databaseName&gt;
 *      &lt;dreAddDataParams&gt;
 *         &lt;param name="(parameter name)"&gt;(parameter value)&lt;/param&gt;
 *      &lt;/dreAddDataParams&gt;
 *      &lt;dreDeleteRefParams&gt;
 *         &lt;param name="(parameter name)"&gt;(parameter value)&lt;/param&gt;
 *      &lt;/dreDeleteRefParams&gt;
 *      &lt;idSourceField keep="[false|true]"&gt;
 *         (Name of source field that will be mapped to the Solr "id" field
 *         or whatever "idTargetField" specified.
 *         Default is the document reference metadata field: 
 *         "document.reference".  Once re-mapped, the metadata source field is 
 *         deleted, unless "keep" is set to <code>true</code>.)
 *      &lt;/idSourceField&gt;
 *      &lt;idTargetField&gt;
 *         (Name of IDOL target field where the store a document unique 
 *         identifier (idSourceField).  If not specified, default 
 *         is "DREREFERENCE".) 
 *      &lt;/idTargetField&gt;
 *      &lt;contentSourceField keep="[false|true]&gt;
 *         (If you wish to use a metadata field to act as the document 
 *         "content", you can specify that field here.  Default 
 *         does not take a metadata field but rather the document content.
 *         Once re-mapped, the metadata source field is deleted,
 *         unless "keep" is set to <code>true</code>.)
 *      &lt;/contentSourceField&gt;
 *      &lt;contentTargetField&gt;
 *         (IDOL target field name for a document content/body.
 *          Default is: DRECONTENT)
 *      &lt;/contentTargetField&gt;
 *      &lt;queueDir&gt;(optional path where to queue files)&lt;/queueDir&gt;
 *      &lt;queueSize&gt;(max queue size before committing)&lt;/queueSize&gt;
 *      &lt;commitBatchSize&gt;
 *          (max number of docs to send IDOL at once)
 *      &lt;/commitBatchSize&gt;
 *   &lt;/committer&gt;
 * </pre>
 *
 * @author Stephen Jacob
 * @author Martin Fournier
 * @author Pascal Essiembre
 */
public class IdolCommitter extends AbstractMappedCommitter {

    private static final long serialVersionUID = -5716008670360409137L;

    private static final Logger LOG = LogManager.getLogger(IdolCommitter.class);

    /** Default key field in Autonomy Idol database. */
    public static final String DEFAULT_IDOL_REFERENCE_FIELD = "DREREFERENCE";
    /** Default field for content in Autonomy Idol Database.*/
    public static final String DEFAULT_IDOL_CONTENT_FIELD = "DRECONTENT";
    /** Default IDOL ACI port. */
    public static final int DEFAULT_ACI_PORT = 9000;
    /** Default IDOL ACI port. */
    public static final int DEFAULT_INDEX_PORT = 9001;

    private final Map<String, String> dreAddDataParams =
            new HashMap<String, String>();
    private final Map<String, String> dreDeleteRefParams =
            new HashMap<String, String>();

    private String databaseName;
    private String host;
    private int aciPort = DEFAULT_ACI_PORT;
    private int indexPort = DEFAULT_INDEX_PORT;


    /**
     * Constructor.
     */
    public IdolCommitter() {
        super();
        setContentTargetField(DEFAULT_IDOL_CONTENT_FIELD);
        setIdTargetField(DEFAULT_IDOL_REFERENCE_FIELD);
    }

    /**
     * Gets IDOL ACI port.
     * @return IDOL ACI port
     */
    public int getAciPort() {
        return aciPort;
    }
    /**
     * Sets IDOL ACI port.
     * @param aciPort IDOL ACI port
     */
    public void setAciPort(int aciPort) {
        this.aciPort = aciPort;
    }

    /**
     * Gets IDOL index port.
     * @return IDOL index port
     */
    public int getIndexPort() {
        return indexPort;
    }
    /**
     * Sets IDOL index port.
     * @param indexPort IDOL index port
     */
    public void setIndexPort(int indexPort) {
        this.indexPort = indexPort;
    }

    /**
     * Gets IDOL database name.
     * @return IDOL database name
     */
    public String getDatabaseName() {
        return databaseName;
    }
    /**
     * Sets IDOL database name.
     * @param databaseName IDOL database name
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Sets IDOL host name.
     * @return IDOL host name
     */
    public String getHost() {
        return host;
    }
    /**
     * Gets IDOL host name.
     * @param host IDOL host name
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Gets the DREADDDATA URL parameter value for the parameter name.
     * @param name parameter name
     * @return parameter value
     */
    public String getDreAddDataParam(String name) {
        return dreAddDataParams.get(name);
    }
    /**
     * Adds a DREADDDATA URL parameter.
     * @param name parameter name
     * @param value parameter value
     */
    public void addDreAddDataParam(String name, String value) {
        dreAddDataParams.put(name, value);
    }
    /**
     * Gets the DREDELETEREF URL parameter value for the parameter name.
     * @param name parameter name
     * @return parameter value
     */
    public String getDreDeleteRefParam(String name) {
        return dreDeleteRefParams.get(name);
    }
    /**
     * Adds the DREDELETEREF URL parameter value for the parameter name.
     * @param name parameter name
     * @param value parameter value
     */
    public void addDreDeleteRefParam(String name, String value) {
        dreDeleteRefParams.put(name, value);
    }
    /**
     * Gets all DREADDDATA URL parameter names
     * @return parameter names
     */
    public Set<String> getDreAddDataParamNames() {
        return dreAddDataParams.keySet();
    }
    /**
     * Gets all DREDELETEREF URL parameter names
     * @return parameter names
     */
    public Set<String> getDreDeleteRefParamNames() {
        return dreDeleteRefParams.keySet();
    }


    @Override
    protected void commitBatch(List<ICommitOperation> batch) {
        LOG.info("Sending " + batch.size() 
                + " documents to IDOL for addition/deletion.");
        List<IAddOperation> additions = new ArrayList<IAddOperation>();
        List<IDeleteOperation> deletions = new ArrayList<IDeleteOperation>();
        for (ICommitOperation op : batch) {
            if (op instanceof IAddOperation) {
                additions.add((IAddOperation) op); 
            } else if (op instanceof IDeleteOperation) {
                deletions.add((IDeleteOperation) op); 
            } else {
                throw new CommitterException("Unsupported operation:" + op);
            }
        }
        dreDeleteRef(deletions);
        dreAddData(additions);
        LOG.info("Done sending documents to IDOL for addition/deletion.");    
        
    }

    @Override
    protected void loadFromXml(XMLConfiguration xml) {
        setHost(xml.getString("host"));
        setAciPort(xml.getInt("aciPort", DEFAULT_ACI_PORT));
        setIndexPort(xml.getInt("indexPort", DEFAULT_INDEX_PORT));
        setDatabaseName(xml.getString("databaseName"));

        List<HierarchicalConfiguration> uparams = xml
                .configurationsAt("dreAddDataParams.param");
        for (HierarchicalConfiguration param : uparams) {
            addDreAddDataParam(param.getString("[@name]"), param.getString(""));
        }

        List<HierarchicalConfiguration> dparams = xml
                .configurationsAt("dreDeleteRefParams.param");
        for (HierarchicalConfiguration param : dparams) {
            addDreDeleteRefParam(
                    param.getString("[@name]"), param.getString(""));
        }
    }

    @Override
    protected void saveToXML(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeStartElement("host");
        writer.writeCharacters(getHost());
        writer.writeEndElement();

        writer.writeStartElement("aciPort");
        writer.writeCharacters(Integer.toString(getAciPort()));
        writer.writeEndElement();
        
        writer.writeStartElement("indexPort");
        writer.writeCharacters(Integer.toString(getIndexPort()));
        writer.writeEndElement();
        
        writer.writeStartElement("databaseName");
        writer.writeCharacters(getDatabaseName());
        writer.writeEndElement();
        
        writer.writeStartElement("dreAddDataParams");
        for (String key : dreAddDataParams.keySet()) {
            writer.writeStartElement("param");
            writer.writeAttribute(key, key);
            writer.writeCharacters(dreAddDataParams.get(key));
            writer.writeEndElement();
        }
        writer.writeEndElement();

        writer.writeStartElement("dreDeleteRefParams");
        for (String key : dreDeleteRefParams.keySet()) {
            writer.writeStartElement("param");
            writer.writeAttribute(key, key);
            writer.writeCharacters(dreDeleteRefParams.get(key));
            writer.writeEndElement();
        }
        writer.writeEndElement();


    }

    /**
     * Build an idol document using the idx file format
     * @param is
     * @param properties
     * @param dbName
     * @return a string containing a document in the idx format
     */
    private String buildIdxDocument(InputStream is, Properties properties) {
        StringBuilder sb = new StringBuilder();
        try {
            // Create a database key for the idol idx document
            sb.append(("\n#DREREFERENCE "));
            sb.append(properties.getString(getIdTargetField()));

            // Loop thru the list of properties and create idx fields
            // accordingly.
            for (Entry<String, List<String>> entry : properties.entrySet()) {
                if (!EqualsUtil.equalsAny(entry.getKey(), 
                        getIdTargetField(), getContentTargetField())) {
                    for (String value : entry.getValue()) {
                        sb.append("\n#DREFIELD ");
                        sb.append(entry.getKey());
                        sb.append("=\"");
                        sb.append(value);
                        sb.append("\"");
                    }
                }
            }
            sb.append("\n#DREDBNAME ");
            sb.append(databaseName);
            sb.append("\n#DRECONTENT\n");
            sb.append(properties.getString(getContentTargetField()));
            sb.append("\n#DREENDDOC ");
            sb.append("\n");
        } finally {
            IOUtils.closeQuietly(is);
        }
        return sb.toString();
    }

    
    /**
     * Commits the addition operations.
     * @param addOperations additions
     */
    public void dreAddData(List<IAddOperation> addOperations) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Sending " + addOperations.size() 
                    + " documents to IDOL for addition.");
        }
        StringBuilder b = new StringBuilder();
        b.append(createURL());
        b.append("DREADDDATA?");
        QueryString qs = new QueryString();
        for (String key : dreAddDataParams.keySet()) {
            qs.addString(key, dreAddDataParams.get(key));
        }
        String addURL = qs.applyOnURL(b.toString());
        String idxBatch = buildIdxBatchContent(addOperations);
        post(addURL, idxBatch);
        LOG.debug("Done sending documents to IDOL for addition.");  
    }

    
    /**
     * Commits the deletion operations
     * @param deleteOperations deletions
     */
    public void dreDeleteRef(List<IDeleteOperation> deleteOperations) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Sending " + deleteOperations.size() 
                    + " documents references to IDOL for deletion.");
        }
        String deleteURL = createURL() + "DREDELETEREF?Docs=" 
                + buildDeleteRefsContent(deleteOperations)
                + "&DREDbName=" + getDatabaseName();
        QueryString qs = new QueryString();
        for (String key : dreDeleteRefParams.keySet()) {
            qs.addString(key, dreDeleteRefParams.get(key));
        }
        String qstring = qs.toString();
        if (StringUtils.isNotBlank(qstring) && qstring.startsWith("&")) {
            deleteURL += "&" + qstring.substring(1);
        }
        post(deleteURL, StringUtils.EMPTY);
        LOG.debug("Done sending references to IDOL for deletion.");   
    }

    /**
     * Perform a DRESYNC / Commit on the idol Database.
     */
    public void sync() {
        String syncURL = createURL() + "DRESYNC";
        post(syncURL, StringUtils.EMPTY);
    }

    /**
     * Creates a HTTP URL connection with the proper Post method and properties.
     * @param url the URL to open
     * @return HttpUrlConnection object
     */
    private HttpURLConnection createURLConnection(String url) {
        URL obj;
        HttpURLConnection con = null;
        try {
            obj = new URL(url);
            con = (HttpURLConnection) obj.openConnection();
            con.setDoInput(true);
            con.setDoOutput(true);
            con.setUseCaches(false);
            con.setRequestProperty(
                    "Content-Type", "application/x-www-form-urlencoded");
            // add request header
            con.setRequestMethod("POST");
            // Send post request
            con.setDoOutput(true);

        } catch (MalformedURLException e) {
            LOG.error("Something went wrong with the URL: " + url, e);
        } catch (IOException e) {
            LOG.error(
                    "I got an I/O problem trying to connect to the server", e);
        }
        return con;
    }

    /**
     * Add/Remove/Commit documents based on the parameters passed to the method.
     * @param url URL to post
     * @param the content to post
     */
    private void post(String url, String content) {
        HttpURLConnection con = null;
        DataOutputStream wr = null;
        try {
            con = createURLConnection(url);
            wr = new DataOutputStream(con.getOutputStream());
            wr.writeBytes(content);
            wr.flush();

            //Get the response
            int responseCode = con.getResponseCode();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sending 'POST' request to URL : " + url);
                LOG.debug("Post parameters : " + content);
                LOG.debug("Server Response Code : " + responseCode);
            }
            String response = IOUtils.toString(con.getInputStream());
            if (!StringUtils.contains(response, "INDEXID")) {
                throw new CommitterException(
                        "Unexpected HTTP response: " + response);
            }
            wr.close();
        } catch (IOException e) {
            throw new CommitterException("Cannot post content to IDOL.", e);
        } finally {
            if (con != null) {
                con.disconnect();
            }
            IOUtils.closeQuietly(wr);
        }
    }

    private String buildDeleteRefsContent(
            List<IDeleteOperation> deleteOperations) {
        StringBuilder  dels = new StringBuilder();
        String sep = StringUtils.EMPTY;
        for (IDeleteOperation op : deleteOperations) {
            try {
                dels.append(sep);
                dels.append(URLEncoder.encode(
                        op.getReference(), CharEncoding.UTF_8));
                sep = "+";
            } catch (IOException e) {
                LOG.error("Could not create deletion references: "
                        + op.getReference(), e);
            }
        }
        return dels.toString();
    }
    
    private String buildIdxBatchContent(List<IAddOperation> addOperations) {
        StringBuilder  idx = new StringBuilder();
        for (IAddOperation op : addOperations) {
            try {
                idx.append(buildIdxDocument(
                        op.getContentStream(), op.getMetadata()));
            } catch (IOException e) {
                LOG.error("Could not create IDX document: "
                        + op.getMetadata(), e);
            }
        }
        idx.append("\n#DREENDDATANOOP\n\n");
        return idx.toString();
    }
    
    private String createURL() {
        StringBuilder url = new StringBuilder();
        // check if the host already has prefix http://
        if (!StringUtils.startsWithAny(host, "http", "https")) {
            url.append("http://");
        }
        url.append(host).append(":").append(indexPort).append("/");
        return url.toString();
    }
    
}