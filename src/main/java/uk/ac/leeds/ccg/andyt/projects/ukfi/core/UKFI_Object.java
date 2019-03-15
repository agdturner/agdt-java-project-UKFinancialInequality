package uk.ac.leeds.ccg.andyt.projects.ukfi.core;

import java.io.Serializable;

/**
 * @author Andy Turner
 */
public abstract class UKFI_Object implements Serializable {

    public transient UKFI_Environment env;
    
    protected UKFI_Object() {
    }

    public UKFI_Object(UKFI_Environment env) {
        this.env = env;
    }
}
