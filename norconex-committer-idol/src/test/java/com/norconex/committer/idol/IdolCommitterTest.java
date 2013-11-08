package com.norconex.committer.idol;

import static org.junit.Assert.*;

import java.io.File;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.norconex.committer.ICommitter;
import com.norconex.commons.lang.config.ConfigurationLoader;
import com.norconex.commons.lang.config.ConfigurationUtil;
import com.norconex.commons.lang.map.Properties;

/* Copyright 2010-2013 Norconex Inc.
 * 
 * This file is part of Norconex Idol Committer.
 * 
 * Norconex Idol Committer is free software: you can redistribute it
 * and/or modify it under the terms of the GNU General Public License as 
 * published by the Free Software Foundation, either version 3 of the License, 
 * or (at your option) any later version.
 * 
 * Norconex Idol Committer is distributed in the hope that it will be 
 * useful, but WITHOUT ANY WARRANTY; without even the implied warranty of 
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with Norconex Idol Committer. 
 * If not, see <http://www.gnu.org/licenses/>.
 */

public class IdolCommitterTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    
    private IdolCommitter committer = null;
    
    private File queue;
    
    @Before
    public void setup() throws Exception {
    	
    	//Create an instance of the Idol committer
    	committer = new IdolCommitter();
    	
    	//TODO:  Setup the Idol Factory
        /*   	
    	*/
    	
    	File configFile = new File("src/test/resources/idolconfig.xml");
    	XMLConfiguration xml = null;
    	XMLConfiguration committerConfig = null;
		try {
			xml = new XMLConfiguration(configFile);
			List<HierarchicalConfiguration> committerNode = xml.configurationsAt("crawlers.crawler.committer");		
			committerConfig = new XMLConfiguration(committerNode.get(0));
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
		
		//Load the xml configuration for the Idol committer
		committer.loadFromXml(committerConfig);
		
		//Setup the queue
        queue = tempFolder.newFolder("queue");
        committer.setQueueDir(queue.toString());
    }
    
	@Test
	public void testXmlLoad() {
		//Verify that the values from the xml file have been loaded into the Idol committer
		assertTrue(committer.getIdolBatchSize() == 15);
		assertTrue(committer.getIdolHost().equalsIgnoreCase("192.168.198.140"));
		assertTrue(committer.getIdolPort() == 9101);
		assertTrue(committer.getUpdateUrlParam("priority").equalsIgnoreCase("100"));
		assertTrue(committer.getDeleteUrlParam("priority").equalsIgnoreCase("100"));
	}

}
