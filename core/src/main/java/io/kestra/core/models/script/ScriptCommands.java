package io.kestra.core.models.script;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public interface ScriptCommands {
    AbstractLogConsumer getLogConsumer();

    List<String> getCommands();

    Map<String, Object> getAdditionalVars();

    Path getWorkingDirectory();

    Map<String, String> getEnv();
}
