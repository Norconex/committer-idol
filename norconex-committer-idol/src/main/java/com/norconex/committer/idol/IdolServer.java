package com.norconex.committer.idol;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Class that helps adding, removing and committing document to idol
 *
 * @author martinfournier
 *
 */
public class IdolServer {
    private static final Logger LOG = LogManager.getLogger(IdolServer.class);

    /**
     * Generate the proper REST url and add the idol document
     *
     * @param url
     * @param idolDocument
     */
    public void add(String url, String idolDocument) {
        url = url.concat("DREADDDATA?");
        HttpURLConnection con = getConnection(url);
        post(con, url, idolDocument);
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

            // add request header
            con.setRequestMethod("POST");
            con.setRequestProperty("User-Agent", "");
            con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");

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
     * @param parameters
     */
    private void post(HttpURLConnection con, String url,
            String parameters) {

        try {
            DataOutputStream wr;
            LOG.debug("Parameter = " + parameters);
            wr = new DataOutputStream(con.getOutputStream());
            wr.writeBytes(parameters);
            wr.flush();
            wr.close();
            int responseCode = con.getResponseCode();
            LOG.debug("\nSending 'POST' request to URL : " + url);
            LOG.debug("Post parameters : " + parameters);
            LOG.debug("Server Response Code : " + responseCode);
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    con.getInputStream(), "UTF-8"));
            String inputLine = null;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
