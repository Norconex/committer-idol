/*
 * Copyright 2010-2014 Norconex Inc.
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

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.norconex.commons.lang.config.ConfigurationUtil;

/**
 * @author Pascal Essiembre
 */
public class IdolCommitterTest {


    @Before
    public void setup() throws Exception {
    }

    @After
    public void teardown() throws Exception {
    }

    @Test
    public void testWriteRead() throws IOException {
        IdolCommitter outCommitter = new IdolCommitter();
        outCommitter.setQueueDir("C:\\FakeTestDirectory\\");
        outCommitter.setContentSourceField("contentSourceField");
        outCommitter.setContentTargetField("contentTargetField");
        outCommitter.setSourceReferenceField("idTargetField");
        outCommitter.setTargetReferenceField("idTargetField");
        outCommitter.setKeepContentSourceField(true);
        outCommitter.setKeepReferenceSourceField(false);
        outCommitter.setQueueSize(100);
        outCommitter.setCommitBatchSize(50);
        outCommitter.setHost("fake.idol.host.com");
        outCommitter.setCfsPort(9100);
        outCommitter.setIndexPort(9001);
        outCommitter.setDatabaseName("Fake Database");
        outCommitter.addDreAddDataParam("aparam1", "avalue1");
        outCommitter.addDreAddDataParam("aparam2", "avalue2");
        outCommitter.addDreDeleteRefParam("dparam1", "dvalue1");
        outCommitter.addDreDeleteRefParam("dparam2", "dvalue2");
        System.out.println("Writing/Reading this: " + outCommitter);
        ConfigurationUtil.assertWriteRead(outCommitter);
    }
}
