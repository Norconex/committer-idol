/*
 * Copyright 2010-2013 Norconex Inc.
 *
 * This file is part of Norconex Idol Committer.
 *
 * Norconex Idol Committer is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 *
 * Norconex Idol Committer is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * Norconex Idol Committer. If not, see <http://www.gnu.org/licenses/>.
 */

package com.norconex.committer.idol;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.norconex.committer.ICommitter;
import com.norconex.commons.lang.map.Properties;

public class IdolCommitterTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private IdolCommitter committer = null;

    private File queue;

    @Before
    public void setup() throws Exception {

        // Create an instance of the Idol committer
        committer = new IdolCommitter();

        // TODO: Setup the Idol Factory
        /*
    	*/

        File configFile = new File("src/test/resources/idolconfig.xml");
        XMLConfiguration xml = null;
        XMLConfiguration committerConfig = null;
        try {
            xml = new XMLConfiguration(configFile);
            List<HierarchicalConfiguration> committerNode = xml
                    .configurationsAt("crawlers.crawler.committer");
            committerConfig = new XMLConfiguration(committerNode.get(0));
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }

        // Load the xml configuration for the Idol committer
        committer.loadFromXml(committerConfig);

        // Setup the queue
        queue = tempFolder.newFolder("queue");
        committer.setQueueDir(queue.toString());

        // Create the databse to do the integration test
        // System.out.println(committer.getIdolDbName());
        // committer.create("test");
    }

    @After
    public void teardown() throws Exception {
        // committer.delete("test");
    }

    @Test
    public void testXmlLoad() {
        // Verify that the values from the xml file have been loaded into the
        // Idol committer
        assertTrue(committer.getIdolBatchSize() == 50);
        assertTrue(committer.getIdolHost().equalsIgnoreCase("192.168.0.202"));
        System.out.println("IdolPort" + committer.getIdolPort());
        assertTrue(committer.getIdolPort() == 9000);
        assertTrue(committer.getIdolIndexPort() == 9001);
        assertTrue(committer.getUpdateUrlParam("priority").equalsIgnoreCase(
                "100"));
        assertTrue(committer.getDeleteUrlParam("priority").equalsIgnoreCase(
                "100"));
    }

    @Test
    public void testCommitAdd() throws Exception {
        String content = "Content1";
        File file = createFile(content);
        String id = "1";
        Properties metadata = new Properties();
        metadata.addString(ICommitter.DEFAULT_DOCUMENT_REFERENCE, id);
        metadata.addString(
                "description",
                "Description 1");
        metadata.addString(
                "keywords",
                "keywords 1");
        metadata.addString("title", "Title 1");

        // Add new doc to Idol
        committer.queueAdd(id, file, metadata);

        String content2 = "Content2";
        File file2 = createFile(content2);
        String id2 = "2";
        Properties metadata2 = new Properties();
        metadata2.addString(ICommitter.DEFAULT_DOCUMENT_REFERENCE, id2);
        metadata2.addString(
                "description",
                "description2 ");
        metadata2.addString(
                "keywords",
                "keyword2");
        metadata2.addString("title", "Title 2");

        // Add new doc to Idol
        committer.queueAdd(id2, file2, metadata2);

        String content3 = "Content3";
        File file3 = createFile(content3);
        String id3 = "3";
        Properties metadata3 = new Properties();
        metadata3.addString(ICommitter.DEFAULT_DOCUMENT_REFERENCE, id3);
        metadata3.addString(
                "description",
                "description3 ");
        metadata3.addString(
                "keywords",
                "keyword3");
        metadata3.addString("title", "Title 3");

        // Add new doc to Idol
        committer.queueAdd(id3, file3, metadata3);

        String content4 = "Content4";
        File file4 = createFile(content4);
        String id4 = "4";
        Properties metadata4 = new Properties();
        metadata4.addString(ICommitter.DEFAULT_DOCUMENT_REFERENCE, id4);
        metadata4.addString(
                "description",
                "description4 ");
        metadata4.addString(
                "keywords",
                "keyword4");
        metadata4.addString("title", "Title 4");

        // Add new doc to Idol
        committer.queueAdd(id4, file4, metadata4);

        committer.commit();

    }

    private File createFile(String content) throws IOException {
        File file = tempFolder.newFile();
        FileWriter writer = new FileWriter(file);
        writer.write(content);
        writer.close();
        return file;
    }

    @Test
    public void testCommitDelete() throws Exception {
        String content = "hello world!";
        File file = createFile(content);
        String id = "2";
        Properties metadata = new Properties();
        metadata.addString(ICommitter.DEFAULT_DOCUMENT_REFERENCE, id);
        metadata.addString(
                "description",
                "Norconex is an enterprise search technology services provider that helps businesses better organize ");
        metadata.addString(
                "keywords",
                "enterprise search, solr, autonomy, attivio, google, microsoft fast, search analytics, search support, e-discovery, web crawler, open-source, taxonomy, metadata, search vendor evaluation, ottawa, gatineau, ontario, quebec, canada");
        metadata.addString("title", "Norconex | Enterprise Search Experts");

        // Remove doc from idol
        committer.queueRemove(id, file, metadata);

        committer.commit();
    }

}
