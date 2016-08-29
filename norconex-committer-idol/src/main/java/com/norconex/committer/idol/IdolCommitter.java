/* Copyright 2010-2016 Norconex Inc.
 *
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
package com.norconex.committer.idol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
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

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.CharEncoding;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.norconex.committer.core.AbstractMappedCommitter;
import com.norconex.committer.core.CommitterException;
import com.norconex.committer.core.IAddOperation;
import com.norconex.committer.core.ICommitOperation;
import com.norconex.committer.core.IDeleteOperation;
import com.norconex.commons.lang.EqualsUtil;
import com.norconex.commons.lang.map.Properties;
import com.norconex.commons.lang.url.QueryString;

/**
 * Commits documents to HP Autonomy IDOL Server/DIH or HP Autonomy Connector 
 * Framework Server (CFS).   Specifying either the index port or the cfs port
 * determines which of the two will be the documents target.
 * <p>
 * XML configuration usage:
 * </p>
 *
 * <pre>
 *   &lt;committer class="com.norconex.committer.idol.IdolCommitter"&gt;
 *      
 *      &lt;!-- To commit documents to IDOL or DIH: --&gt;
 *      &lt;host&gt;(IDOL/DIH host name or IP)&lt;/host&gt;
 *      &lt;indexPort&gt;(IDOL/DIH index port)&lt;/indexPort&gt;
 *      &lt;databaseName&gt;(Optional IDOL Database Name where to store documents)&lt;/databaseName&gt;
 *      &lt;dreAddDataParams&gt;
 *         &lt;param name="(parameter name)"&gt;(parameter value)&lt;/param&gt;
 *      &lt;/dreAddDataParams&gt;
 *      &lt;dreDeleteRefParams&gt;
 *         &lt;param name="(parameter name)"&gt;(parameter value)&lt;/param&gt;
 *      &lt;/dreDeleteRefParams&gt;
 *
 *      &lt;!-- To commit documents to CFS: --&gt;
 *      &lt;host&gt;(CFS host name or IP)&lt;/host&gt;
 *      &lt;cfsPort&gt;(CFS Server/Ingest port)&lt;/cfsPort&gt;
 *
 *      &lt;!-- Common settings: --&gt;
 *      &lt;sourceReferenceField keep="[false|true]"&gt;
 *         (Optional name of field that contains the document reference, when 
 *         the default document reference is not used.  The reference value
 *         will be mapped to the IDOL "DREREFERENCE" field, or the 
 *         "targetReferenceField" specified.
 *         Once re-mapped, this metadata source field is 
 *         deleted, unless "keep" is set to <code>true</code>.)
 *      &lt;/sourceReferenceField&gt;
 *      &lt;targetReferenceField&gt;
 *         (Optional name of IDOL target field where to store the source 
 *         reference. If not specified, default is "DREREFERENCE".) 
 *      &lt;/targetReferenceField&gt;
 *      &lt;sourceContentField keep="[false|true]"&gt;
 *         (If you wish to use a metadata field to act as the document 
 *         "content", you can specify that field here.  Default 
 *         does not take a metadata field but rather the document content.
 *         Once re-mapped, the metadata source field is deleted,
 *         unless "keep" is set to <code>true</code>.)
 *      &lt;/sourceContentField&gt;
 *      &lt;targetContentField&gt;
 *         (IDOL target field name for a document content/body.
 *          Default is: DRECONTENT)
 *      &lt;/targetContentField&gt;
 *      &lt;commitBatchSize&gt;
 *          (max number of docs to send IDOL at once)
 *      &lt;/commitBatchSize&gt;
 *      &lt;queueDir&gt;(optional path where to queue files)&lt;/queueDir&gt;
 *      &lt;queueSize&gt;(max queue size before committing)&lt;/queueSize&gt;
 *      &lt;maxRetries&gt;(max retries upon commit failures)&lt;/maxRetries&gt;
 *      &lt;maxRetryWait&gt;(max delay between retries)&lt;/maxRetryWait&gt;
 *   &lt;/committer&gt;
 * </pre>
 *
 * @author Stephen Jacob
 * @author Martin Fournier
 * @author Pascal Essiembre
 */
public class IdolCommitter extends AbstractMappedCommitter {

    private static final Logger LOG = LogManager.getLogger(IdolCommitter.class);

    /** Default key field in Autonomy Idol database. */
    public static final String DEFAULT_IDOL_REFERENCE_FIELD = "DREREFERENCE";
    /** Default field for content in Autonomy Idol Database.*/
    public static final String DEFAULT_IDOL_CONTENT_FIELD = "DRECONTENT";

    private final Map<String, String> dreAddDataParams =
            new HashMap<String, String>();
    private final Map<String, String> dreDeleteRefParams =
            new HashMap<String, String>();

    private String databaseName;
    private String host;
    private int cfsPort = -1;
    private int indexPort = -1;

    /**
     * Constructor.
     */
    public IdolCommitter() {
        super();
        setTargetContentField(DEFAULT_IDOL_CONTENT_FIELD);
        setTargetReferenceField(DEFAULT_IDOL_REFERENCE_FIELD);
    }

    /**
     * Gets CFS port.
     * @return CFS port
     */
    public int getCfsPort() {
        return cfsPort;
    }
    /**
     * Sets CFS port.
     * @param cfsPort CFS port
     */
    public void setCfsPort(int cfsPort) {
        this.cfsPort = cfsPort;
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
        // validate settings first
        if ((cfsPort < 0 && indexPort < 0) 
                || (cfsPort >= 0 && indexPort >= 0)) {
            throw new CommitterException(
                    "One (and only one) of CFS Port or Index Port must "
                  + "be specified.");
        }
        
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
    }

    @Override
    protected void loadFromXml(XMLConfiguration xml) {
        setHost(xml.getString("host"));
        setCfsPort(xml.getInt("cfsPort", -1));
        setIndexPort(xml.getInt("indexPort", -1));
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

        writer.writeStartElement("cfsPort");
        writer.writeCharacters(Integer.toString(getCfsPort()));
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
     * @param is input stream
     * @param properties properties
     * @return a string containing a document in the idx format
     */
    protected String buildIdxDocument(InputStream is, Properties properties) {
        StringBuilder sb = new StringBuilder();
        try {
            // Create a database key for the idol idx document
            String targetIdField = getTargetReferenceField();
            if (DEFAULT_IDOL_REFERENCE_FIELD.equalsIgnoreCase(targetIdField)) {
                sb.append(("\n#DREREFERENCE "));
                sb.append(properties.getString(targetIdField));
            } else {
                appendField(
                        sb, targetIdField, properties.getString(targetIdField));
            }
            
            // Loop thru the list of properties and create idx fields
            // accordingly.
            for (Entry<String, List<String>> entry : properties.entrySet()) {
                if (!EqualsUtil.equalsAny(entry.getKey(), 
                        getTargetReferenceField(), getTargetContentField())) {
                    for (String value : entry.getValue()) {
                        appendField(sb, entry.getKey(), value);
                    }
                }
            }
            if (StringUtils.isNotBlank(databaseName)) {
                sb.append("\n#DREDBNAME ");
                sb.append(databaseName);
            }

            // Store content at specified location
            String targetCtntField = getTargetContentField();
            if (DEFAULT_IDOL_CONTENT_FIELD.equalsIgnoreCase(targetCtntField)) {
                sb.append("\n#DRECONTENT\n");
                sb.append(properties.getString(targetCtntField));
                sb.append("\n#DREENDDOC ");
            } else {
                appendField(sb, targetCtntField, 
                        properties.getString(targetCtntField));
            }
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
        if (addOperations.isEmpty()) {
            return;
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("Sending " + addOperations.size() 
                    + " documents for addition to " + createURL());
        }
        
        StringBuilder b = new StringBuilder();
        b.append(createURL());
        
        if (isCFS()) {
            b.append("action=ingest&adds=");
            StringWriter xml = new StringWriter();
            XMLOutputFactory factory = XMLOutputFactory.newInstance();
            try {
                XMLStreamWriter writer = factory.createXMLStreamWriter(xml);
                writer.writeStartElement("adds");
                buildCfsXmlBatchContent(writer, addOperations);
                writer.writeEndElement();
                writer.flush();
                writer.close();
                b.append(URLEncoder.encode(xml.toString(), CharEncoding.UTF_8));
            } catch (Exception e) {
                throw new CommitterException("Cannot create XML.", e);
            }   
            postToIDOL(b.toString(), StringUtils.EMPTY);
        } else {
            b.append("DREADDDATA?");
            QueryString qs = new QueryString();
            for (String key : dreAddDataParams.keySet()) {
                qs.addString(key, dreAddDataParams.get(key));
            }
            String addURL = qs.applyOnURL(b.toString());
            String idxBatch = buildIdxBatchContent(addOperations);
            postToIDOL(addURL, idxBatch);
        }
        if (LOG.isInfoEnabled()) {
            LOG.debug("Done sending additions to " + createURL());  
        }
    }

    
    /**
     * Commits the deletion operations
     * @param deleteOperations deletions
     */
    public void dreDeleteRef(List<IDeleteOperation> deleteOperations) {
        if (deleteOperations.isEmpty()) {
            return;
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("Sending " + deleteOperations.size() 
                    + " documents for deletion to " + createURL());
        }
        
        String deleteURL = createURL();
        if (isCFS()) {
            deleteURL += "action=ingest&removes="
                    + buildDeleteRefsContent(deleteOperations, ",");
        } else {
            deleteURL += "DREDELETEREF?Docs=" 
                    + buildDeleteRefsContent(deleteOperations, "+")
                    + "&DREDbName=" + getDatabaseName();
            QueryString qs = new QueryString();
            for (String key : dreDeleteRefParams.keySet()) {
                qs.addString(key, dreDeleteRefParams.get(key));
            }
            String qstring = qs.toString();
            if (StringUtils.isNotBlank(qstring) && qstring.startsWith("&")) {
                deleteURL += "&" + qstring.substring(1);
            }
        }
        postToIDOL(deleteURL, StringUtils.EMPTY);
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Done sending deletions to " + createURL());   
        }
    }

    /**
     * Perform a DRESYNC / Commit on the idol Database.
     */
    public void sync() {
        String syncURL = createURL() + "DRESYNC";
        postToIDOL(syncURL, StringUtils.EMPTY);
    }

    private void appendField(StringBuilder sb, String name, String value) {
        sb.append("\n#DREFIELD ");
        sb.append(name);
        sb.append("=\"");
        sb.append(value);
        sb.append("\"");
    }
    
    /**
     * Creates a HTTP URL connection with the proper Post method and properties.
     * @param url the URL to open
     * @return HttpUrlConnection object
     */
    private HttpURLConnection createURLConnection(String url) {
        URL targetURL;
        HttpURLConnection con = null;
        try {
            targetURL = new URL(url);
            con = (HttpURLConnection) targetURL.openConnection();
            con.setDoInput(true);
            con.setDoOutput(true);
            con.setUseCaches(false);
            con.setRequestProperty(
                    "Content-Type", "application/x-www-form-urlencoded");
            // add request header
            con.setRequestMethod("POST");
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
    private void postToIDOL(String url, String content) {
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
            String response = IOUtils.toString(
                    con.getInputStream(), CharEncoding.UTF_8);
            if ((isCFS() && !StringUtils.contains(response, "SUCCESS"))
                    || (!isCFS() 
                            && !StringUtils.contains(response, "INDEXID"))) {
                throw new CommitterException(
                        "Unexpected HTTP response: " + response);
            }
            wr.close();
        } catch (IOException e) {
            throw new CommitterException(
                    "Cannot post content to " + createURL(), e);
        } finally {
            if (con != null) {
                con.disconnect();
            }
            IOUtils.closeQuietly(wr);
        }
    }

    private String buildDeleteRefsContent(
            List<IDeleteOperation> deleteOperations, String joinCharacter) {
        StringBuilder  dels = new StringBuilder();
        String sep = StringUtils.EMPTY;
        for (IDeleteOperation op : deleteOperations) {
            try {
                dels.append(sep);
                dels.append(URLEncoder.encode(
                        op.getReference(), CharEncoding.UTF_8));
                sep = joinCharacter;
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
    
    private void buildCfsXmlBatchContent(
            XMLStreamWriter writer, List<IAddOperation> addOperations) {
        for (IAddOperation op : addOperations) {
            try {
                buildCfsXmlDocument(
                        writer, op.getContentStream(), op.getMetadata());
            } catch (Exception e) {
                LOG.error("Could not create XML document: "
                        + op.getMetadata(), e);
            }
        }
    }
    protected void buildCfsXmlDocument(
            XMLStreamWriter writer, InputStream is, Properties properties)
                    throws XMLStreamException, IOException {
        try {
            writer.writeStartElement("add");
            writer.writeStartElement("document");

            // Create a database key for the idol XML document
            String targetIdField = getTargetReferenceField();
            if (DEFAULT_IDOL_REFERENCE_FIELD.equalsIgnoreCase(targetIdField)) {
                writer.writeStartElement("reference");
                writer.writeCharacters(properties.getString(targetIdField));
                writer.writeEndElement();
            } else {
                writer.writeStartElement("metadata");
                writer.writeAttribute("name", targetIdField);
                writer.writeAttribute(
                        "value", properties.getString(targetIdField));
                writer.writeEndElement();
            }
            
            // Loop thru the list of properties and create XML fields
            // accordingly.
            for (Entry<String, List<String>> entry : properties.entrySet()) {
                if (!EqualsUtil.equalsAny(entry.getKey(), 
                        getTargetReferenceField(), getTargetContentField())) {
                    for (String value : entry.getValue()) {
                        writer.writeStartElement("metadata");
                        writer.writeAttribute("name", entry.getKey());
                        writer.writeAttribute("value", value);
                        writer.writeEndElement();
                    }
                }
            }

            // Store content at specified location
            String targetCtntField = getTargetContentField();
            String targetCtntValue = properties.getString(targetCtntField);

            writer.writeEndElement();

            writer.writeStartElement("source");
            writer.writeAttribute("content", Base64.encodeBase64String(
                    targetCtntValue.getBytes(CharEncoding.UTF_8)));
            writer.writeEndElement();

            writer.writeEndElement();
        } finally {
            IOUtils.closeQuietly(is);
        }
    }
    
    
    private String createURL() {
        StringBuilder url = new StringBuilder();
        // check if the host already has prefix http://
        if (!StringUtils.startsWithAny(host, "http", "https")) {
            url.append("http://");
        }
        url.append(host).append(":");
        if (isCFS()) {
            url.append(cfsPort);
        } else {
            url.append(indexPort);
        }
        url.append("/");
        return url.toString();
    }
    
    private boolean isCFS() {
        return cfsPort > 0;
    }
}