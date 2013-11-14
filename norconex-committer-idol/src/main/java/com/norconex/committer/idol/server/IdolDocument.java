package com.norconex.committer.idol.server;

import java.io.File;

import com.norconex.commons.lang.map.Properties;

public class IdolDocument {
	private String drereference;
    private String dretitle;
    private String dredate;
    private String dredbname;
    private String drefilename;
    private String drestorecontent;
    private String importMagicExtension;
    private String importMagicFriendlyType;
    private String drecontent;
    
    public IdolDocument(String id,File file ,Properties metadata){
    	this.drereference=id;
    	this.drecontent = id;
    }
}
