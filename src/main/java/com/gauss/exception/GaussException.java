package com.gauss.exception;

import org.apache.commons.lang.exception.NestableRuntimeException;

public class GaussException extends NestableRuntimeException {

    private static final long serialVersionUID = -654893533794556357L;

    public GaussException(String errorCode){
        super(errorCode);
    }

    public GaussException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public GaussException(String errorCode, String errorDesc){
        super(errorCode + ":" + errorDesc);
    }

    public GaussException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode + ":" + errorDesc, cause);
    }

    public GaussException(Throwable cause){
        super(cause);
    }

}
