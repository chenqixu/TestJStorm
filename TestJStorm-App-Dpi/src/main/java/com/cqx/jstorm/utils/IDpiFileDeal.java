package com.cqx.jstorm.utils;

/**
 * IDpiFileDeal
 *
 * @author chenqixu
 */
public interface IDpiFileDeal {
    void run(String value) throws Exception;
    void end() throws Exception;
}
