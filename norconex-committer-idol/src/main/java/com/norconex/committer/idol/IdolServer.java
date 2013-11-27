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

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.norconex.committer.FileSystemQueueCommitter.QueuedAddedDocument;
import com.norconex.commons.lang.map.Properties;

/**
 * Class that helps adding, removing and committing document to idol
 *
 * @author Martin Fournier
 */
public class IdolServer {
    private static final Logger LOG = LogManager.getLogger(IdolServer.class);
    String idolDocuments = "";

    /**
     * Generate the proper REST url and add the idol document
     *
     * @param url
     * @param idolDocument
     */

    public void add(String url, List<QueuedAddedDocument> docsToAdd,String databaseName) {
        url = url.concat("DREADDDATA?");
        LOG.trace("In method add and we have "+ docsToAdd.size() + " documents to process");
        String idolDocumentsBatched = buildIdolDocumentsBatch(docsToAdd,databaseName);
        HttpURLConnection con = getConnection(url);
        post(con, url, idolDocumentsBatched);
        LOG.trace("Idol Document Batched " + idolDocumentsBatched);
    }

    /**
     * Generate the proper REST url and delete the idol document
     *
     * @param url
     * @param reference
     * @param dreDbName
     */
    public void delete(String url, String reference, String dreDbName) {
        url = url.concat("DREDELETEREF?Docs=" + reference + "&DREDbName="
                + dreDbName);
        HttpURLConnection con = getConnection(url);
        post(con, url, reference);
    }

    /**
     * Perform a DRESYNC / Commit on the idol Database.
     *
     * @param url
     */
    public void sync(String url) {
        url = url.concat("DRESYNC");
        HttpURLConnection con = getConnection(url);
        post(con, url, "");
    }

    /**
     * Return a HTTP connection with the proper Post method and properties.
     *
     * @param url
     * @return HttpUrlConnection object
     */
    private HttpURLConnection getConnection(String url) {
        URL obj;
        HttpURLConnection con = null;
        try {
            obj = new URL(url);
            con = (HttpURLConnection) obj.openConnection();
            con.setDoInput(true);
            con.setDoOutput(true);
            con.setUseCaches(false);
            con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            // add request header
            con.setRequestMethod("POST");

            // Send post request
            con.setDoOutput(true);

        } catch (MalformedURLException e) {
            LOG.error("Something went wrong with the URL: " + url);
            e.printStackTrace();
        } catch (IOException e) {
            LOG.error("I got an I/O problem trying to connect to the server");
            e.printStackTrace();
        }
        return con;
    }

    /**
     * Add/Remove/Commit documents based on the parameters passed to the method.
     *
     * @param con
     * @param url
     * @param content
     */
    private void post(HttpURLConnection con, String url,
            String content) {

        try {
            DataOutputStream wr;
            wr = new DataOutputStream(con.getOutputStream());
            wr.writeBytes(content);
            wr.flush();

            //Get the response
            int responseCode = con.getResponseCode();
            LOG.trace("\nSending 'POST' request to URL : " + url);
            LOG.trace("Post parameters : " + content);
            LOG.trace("Server Response Code : " + responseCode);
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    con.getInputStream(), "UTF-8"));
            List responseLines = new ArrayList();
            String line = null;

            while((line = in.readLine())!=null){
                if(line != null && line.trim().length()>0){
                    if(LOG.isDebugEnabled()){
                        LOG.debug("Response line: "+line);
                        responseLines.add(line);
                    }
                }
            }

            in.close();
            wr.close();
            String response = StringUtils.join(responseLines.iterator()," ");
            if(!StringUtils.contains(response, "INDEXID")){
                throw new RuntimeException("Unexpected HTTP response: " + response);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String buildIdolDocumentsBatch(List<QueuedAddedDocument> docsToAdd,String databaseName) {
        LOG.debug("In method builIdolDocumentsBatch and we have " + docsToAdd.size() + " documents to process");
        StringBuilder  idolDocumentBatched = new StringBuilder();
        for (QueuedAddedDocument qad : docsToAdd) {
            try {
                idolDocumentBatched.append(buildIdolDocument(qad.getContentStream(), qad.getMetadata(),databaseName));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        //TODO the closing argument should be configured by parameter
        idolDocumentBatched.append("\n#DREENDDATANOOP\n\n");
        return idolDocumentBatched.toString();
    }
    /**
     *This method gets the value that will be used as key in the Idol Database.
     * @param prop
     * @return String containing the text that will be used as key in the Idol Database
     */
    private String getDreReference(Properties prop) {
        LOG.trace("In method getDreReference");
        String dreReferenceValue = "";
        for (Entry<String, List<String>> entry : prop.entrySet()) {
            for (String value : entry.getValue()) {
                LOG.trace("value: " + value);
                if (entry.getKey().equals("document.reference")) {
                    dreReferenceValue = value;
                }
            }
        }
        LOG.trace("dreReference " + dreReferenceValue);
        return dreReferenceValue;
    }

    /**
     * Build an idol document using the idx file format
     * @param is
     * @param properties
     * @param dbName
     * @return a string containing a document in the idx format
     */
    private String buildIdolDocument(InputStream is,
            Properties properties, String dbName) {
        StringBuilder sb = new StringBuilder();
        try {
            // Create a database key for the idol idx document
            sb.append(("\n#DREREFERENCE "));
            sb.append(getDreReference(properties));

            // Loop thru the list of properties and create idx fields
            // accordingly.
            for (Entry<String, List<String>> entry : properties.entrySet()) {
                for (String value : entry.getValue()) {
                    sb.append("\n#DREFIELD ");
                    sb.append(entry.getKey());
                    sb.append("=\"");
                    sb.append(value);
                    sb.append("\"");
                }
            }
            sb.append("\n#DREDBNAME ");
            sb.append(dbName);
            sb.append("\n#DRECONTENT\n");
            sb.append(IOUtils.toString(is));
            sb.append("\n#DREENDDOC ");
            sb.append("\n");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(is);
        }
        return sb.toString();
    }
}
