package com.cqx.jstorm.bean;

/**
 * DpiFile
 *
 * @author chenqixu
 */
public class DpiFile {
    private String filename;
    private TypeDef typeDef;

    public DpiFile() {
    }

    public DpiFile(String filename, TypeDef typeDef) {
        this.filename = filename;
        this.typeDef = typeDef;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public TypeDef getTypeDef() {
        return typeDef;
    }

    public void setTypeDef(TypeDef typeDef) {
        this.typeDef = typeDef;
    }
}
