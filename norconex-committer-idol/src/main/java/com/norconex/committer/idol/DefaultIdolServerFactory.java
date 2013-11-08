/* Copyright 2013 Norconex Inc.
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
package com.norconex.committer.idol;

import org.apache.commons.lang3.StringUtils;

import com.norconex.committer.CommitterException;
import com.norconex.committer.idol.server.IdolServer;

/**
 * 
 * Implementation that creates an Idol Server instance.
 * 
 * @author Stephen Jacob
 * 
 */
public class DefaultIdolServerFactory implements IIdolServerFactory {

    private static final long serialVersionUID = 5820720860417411567L;
    private IdolServer server;
    
	public IdolServer createIdolServer(IdolCommitter idolCommitter) {
        /*
		if (server == null) {
            if (StringUtils.isBlank(idolCommitter.getIdolUrl())) {
                throw new CommitterException("Idol URL is undefined.");
            }
            server = new IdolServer(idolCommitter.getIdolHost(),idolCommitter.getIdolPort());
        }
        return server;
        */
		return null;
	}
	
    @Override
    public String toString() {
        return "DefaultIdolServerFactory [server=" + server + "]";
    }
}
