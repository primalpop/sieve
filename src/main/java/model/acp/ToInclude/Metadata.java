package model.acp.ToInclude;

import java.sql.Date;

/**
 * Created by cygnus on 9/25/17.
 */
public abstract class Metadata {

    private String author;

    private Date created;

    private Date lastEdited;

    public String getAuthor(){
        return author;
    }

    public Date getCreated(){
        return created;
    }

    public Date getLastEdited(){
        return lastEdited;
    }

}
