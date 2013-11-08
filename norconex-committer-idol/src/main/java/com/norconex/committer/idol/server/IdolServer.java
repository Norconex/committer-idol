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
package com.norconex.committer.idol.server;

import java.io.Serializable;

/**
 * @author Stephen Jacob
 *
 */
public abstract class IdolServer implements Serializable {

	public abstract IdolResponse add(IdolDocument doc);
	
}
