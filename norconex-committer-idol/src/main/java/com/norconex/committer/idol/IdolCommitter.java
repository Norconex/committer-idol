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

import java.io.IOException;
import java.io.InputStream;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.norconex.committer.BaseCommitter;
import com.norconex.committer.CommitterException;
import com.norconex.commons.lang.config.IXMLConfigurable;
import com.norconex.commons.lang.map.Properties;

/**
 * Commits documents to Autonomy IDOL Server via a rest api.
 * <p>
 * XML configuration usage:
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
 * @author Stephen Jacob
 */
public class IdolCommitter extends BaseCommitter implements IXMLConfigurable {

    private static final long serialVersionUID = 1;

    /**
     * Logging object to be uses to output debug/info/warning information.
     */
    private static final Logger LOG = LogManager.getLogger(IdolCommitter.class);

    /**
     * DREREFERENCE is the default key field in Autonomy Idol database.
     */
    public static final String DEFAULT_IDOL_REF_FIELD = "DREREFERENCE";
    /**
     * DRECONTENT is the default field for content in Autonomy Idol Database.
     */
    public static final String DEFAULT_IDOL_CONTENT_FIELD = "DRECONTENT";

    public static final int DEFAULT_IDOL_BATCH_SIZE = 100;
    public static final int DEFAULT_IDOL_PORT = 9000;
    public static final int DEFAULT_IDOL_INDEX_PORT = 9001;

    private final List<QueuedAddedDocument> docsToAdd =
            new ArrayList<QueuedAddedDocument>();
    private final List<QueuedDeletedDocument> docsToRemove =
            new ArrayList<QueuedDeletedDocument>();
    private final Map<String, String> updateUrlParams =
            new HashMap<String, String>();
    private final Map<String, String> deleteUrlParams =
            new HashMap<String, String>();

    private int idolBatchSize = DEFAULT_IDOL_BATCH_SIZE;
    private String idolDbName;
    private String idolHost;
    private int idolPort = DEFAULT_IDOL_PORT;
    private int idolIndexPort = DEFAULT_IDOL_INDEX_PORT;
    Object QueueAddLock = new Object();
    Object QueueDeleteLock = new Object();


    public IdolCommitter() {
        super();
    }
    public IdolCommitter(int batchSize) {
        super(batchSize);
    }

    int getIdolIndexPort() {
        return idolIndexPort;
    }
    public void setIdolIndexPort(int idolIndexPort) {
        this.idolIndexPort = idolIndexPort;
    }

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

    public String getUpdateUrlParam(String name) {
        return updateUrlParams.get(name);
    }
    public void setUpdateUrlParam(String name, String value) {
        updateUrlParams.put(name, value);
    }

    public String getDeleteUrlParam(String name) {
        return deleteUrlParams.get(name);
    }
    public void setDeleteUrlParam(String name, String value) {
        deleteUrlParams.put(name, value);
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

    public String getIdolUrl() {
        StringBuilder url = new StringBuilder();
        // check if the host already has prefix http://
        if (!StringUtils.startsWithAny(idolHost, "http", "https")) {
            url.append("http://");
        }
        url.append(getIdolHost() + ":" + getIdolIndexPort() + "/");
        return url.toString();
    }

    private String getDreReference(Properties prop) {
        String dreReferenceValue = "";
        for (Entry<String, List<String>> entry : prop.entrySet()) {
            for (String value : entry.getValue()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("value: " + value);
                }
                if (entry.getKey().equals("document.reference")) {
                    dreReferenceValue = value;
                }
            }
        }
        return dreReferenceValue;
    }

    /**
     * Builds an idol document. An example of an idol document would look like
     * this: <br>
     * #DREREFERENCE 1 <br>
     * #DRETITLE Title Goes Here <br>
     * #DRECONTENT<br>
     * Content Goes Here <br>
     * #DREDBNAME test <br>
     * #DREENDDOC <br>
     * #DREENDDATAREFERENCE
     *
     * @param is
     * @param properties
     * @return idolDocument
     */
    private String buildIdolDocument(InputStream is, Properties properties) {
        StringBuilder doc = new StringBuilder();
        try {
            // Create a database key for the idol idx document
            doc.append("\n#DREREFERENCE ");
            doc.append(this.getDreReference(properties));

            // Loop thru the list of properties and create idx fields
            // accordingly.
            for (Entry<String, List<String>> entry : properties.entrySet()) {
                for (String value : entry.getValue()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("value: " + value);
                    }
                    doc.append("\n#DREFIELD ");
                    doc.append(entry.getKey());
                    doc.append("=\"").append(value).append("\"");
                }
            }
            doc.append("\n#DREDBNAME ").append(this.getIdolDbName());
            doc.append("\n#DRECONTENT\n").append(IOUtils.toString(is));
            doc.append("\n#DREENDDOC ");
            doc.append("\n#DREENDDATAREFERENCE");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(is);
        }
        return doc.toString();
    }

    private void addToIdol(InputStream is, Properties properties)
            throws IOException {
        String idolDocument = buildIdolDocument(is, properties);
        IdolServer idolServer = new IdolServer();
        idolServer.add(this.getIdolUrl(), docsToAdd,this.getIdolDbName());
        idolServer.sync(this.getIdolUrl());

    }

    private void delFromIdol(String reference, String dreDbName) {
        IdolServer idolServer = new IdolServer();
        idolServer.delete(this.getIdolUrl(), reference, dreDbName);
        idolServer.sync(this.getIdolUrl());
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

    private void persistToIdol() {
        LOG.info("Sending " + docsToAdd.size()
                + " documents to Idol for update.");
        // making sure the list is thread safe
        synchronized (QueueAddLock) {
            for (QueuedAddedDocument qad : docsToAdd) {
                try {
                    this.addToIdol(qad.getContentStream(), qad.getMetadata());
                    LOG.debug(qad.getMetadata());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // Delete queued documents after commit
        for (QueuedAddedDocument doc : docsToAdd) {
            doc.deleteFromQueue();
        }
        docsToAdd.clear();

        LOG.info("Done sending documents to Idol for update.");
    }

    private void deleteFromIdol() {
        LOG.info("Sending " + docsToRemove.size()
                + " documents to Idol for deletion.");
        // Making sure the list is thread safe
        synchronized (QueueDeleteLock) {
            for (QueuedDeletedDocument doc : docsToRemove) {
                try {
                    this.delFromIdol(doc.getReference(), idolDbName);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            try {
                for (QueuedDeletedDocument doc : docsToRemove) {
                    doc.deleteFromQueue();
                }
                docsToRemove.clear();
            } catch (Exception e) {
                throw new CommitterException(
                        "Cannot delete document batch from Idol.", e);
            }
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