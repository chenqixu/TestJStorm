package com.cqx.jstorm.base;

import org.junit.Test;

public class TestJStormAgentTest {

    @Test
    public void localsubmit() throws Exception {
        String[] _args = new String[]{"--conf", "D:\\Document\\Workspaces\\Git\\TestJStorm\\TestJStorm-Agent\\src\\main\\resources\\config.local.yaml",
                "--type", "localsubmit",
                "--jarpath", "D:\\Document\\Workspaces\\Git\\TestJStorm\\target"
        };
        TestJStormAgent.builder().run(_args);
    }

    @Test
    public void submit() throws Exception {
        String[] _args = new String[]{"--conf", "D:\\Document\\Workspaces\\Git\\TestJStorm\\TestJStorm-Agent\\src\\main\\resources\\config.yaml",
                "--type", "submit",
                "--jarpath", "D:\\Document\\Workspaces\\Git\\TestJStorm\\target"
        };
        TestJStormAgent.builder().run(_args);
    }

    @Test
    public void kill() throws Exception {
        String[] _args = new String[]{"--conf", "D:\\Document\\Workspaces\\Git\\TestJStorm\\TestJStorm-Agent\\src\\main\\resources\\config.yaml",
                "--type", "kill",
                "--jarpath", "D:\\Document\\Workspaces\\Git\\TestJStorm\\target"
        };
        TestJStormAgent.builder().run(_args);
    }
}