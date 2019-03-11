package com.cqx.jstorm.base;

import org.junit.Test;

public class TestJStormAgentTest {

    @Test
    public void submit() throws Exception {
        String[] _args = new String[]{"--conf", "D:\\Document\\Workspaces\\Git\\TestJStorm\\TestJStorm-Agent\\src\\main\\resources\\conf\\config.yaml",
                "--type", "submit"};
        TestJStormAgent.builder().run(_args);
    }

    @Test
    public void kill() throws Exception {
        String[] _args = new String[]{"--conf", "D:\\Document\\Workspaces\\Git\\TestJStorm\\TestJStorm-Agent\\src\\main\\resources\\conf\\config.yaml",
                "--type", "kill"};
        TestJStormAgent.builder().run(_args);
    }
}