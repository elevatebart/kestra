package org.kestra.webserver.controllers;

import io.micronaut.context.ApplicationContext;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.exceptions.HttpStatusException;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.validation.Validated;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.kestra.core.contexts.KestraApplicationContext;
import org.kestra.core.docs.DocumentationGenerator;
import org.kestra.core.docs.ClassPluginDocumentation;
import org.kestra.core.models.tasks.FlowableTask;
import org.kestra.core.plugins.PluginRegistry;
import org.kestra.core.plugins.PluginScanner;
import org.kestra.core.plugins.RegisteredPlugin;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;

@Validated
@Controller("/api/v1/plugins/")
public class PluginController {
    @Inject
    private ApplicationContext applicationContext;

    @Get
    @ExecuteOn(TaskExecutors.IO)
    public List<Plugin> search() throws HttpStatusException {
        return plugins()
            .stream()
            .map(Plugin::of)
            .collect(Collectors.toList());
    }

    @Get(uri = "icons")
    public Map<String, PluginIcon> icons() throws HttpStatusException {
        return plugins()
            .stream()
            .flatMap(plugin -> Stream
                .concat(
                    plugin.getTasks().stream(),
                    Stream.concat(
                        plugin.getTriggers().stream(),
                        plugin.getConditions().stream()
                    )
                )
                .map(e -> new AbstractMap.SimpleEntry<>(
                    e.getName(),
                    new PluginIcon(
                        e.getSimpleName(),
                        DocumentationGenerator.icon(plugin, e),
                        FlowableTask.class.isAssignableFrom(e)
                    )
                ))
            )
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Get(uri = "{cls}")
    @ExecuteOn(TaskExecutors.IO)
    public Doc pluginDocumentation(String cls) throws HttpStatusException, IOException {
        ClassPluginDocumentation classPluginDocumentation = pluginDocumentation(plugins(), cls);

        return new Doc(
            DocumentationGenerator.render(classPluginDocumentation),
            new Schema(
                classPluginDocumentation.getPropertiesSchema(),
                classPluginDocumentation.getOutputsSchema()
            )
        );
    }

    private List<RegisteredPlugin> plugins() {
        if (!(applicationContext instanceof KestraApplicationContext)) {
            throw new RuntimeException("Invalid ApplicationContext");
        }

        KestraApplicationContext context = (KestraApplicationContext) applicationContext;
        PluginRegistry pluginRegistry = context.getPluginRegistry();

        List<RegisteredPlugin> plugins = new ArrayList<>();
        if (pluginRegistry != null) {
            plugins = new ArrayList<>(pluginRegistry.getPlugins());
        }

        PluginScanner corePluginScanner = new PluginScanner(PluginController.class.getClassLoader());
        plugins.add(corePluginScanner.scan());

        return plugins;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private ClassPluginDocumentation<?> pluginDocumentation(List<RegisteredPlugin> plugins, String className)  {
        RegisteredPlugin registeredPlugin = plugins
            .stream()
            .filter(r -> r.hasClass(className))
            .findFirst()
            .orElseThrow(() -> new NoSuchElementException("Class '" + className + "' doesn't exists "));

        Class cls = registeredPlugin
            .findClass(className)
            .orElseThrow(() -> new NoSuchElementException("Class '" + className + "' doesn't exists "));

        Class baseCls = registeredPlugin
            .baseClass(className);

        return ClassPluginDocumentation.of(registeredPlugin, cls, baseCls);
    }

    @NoArgsConstructor
    @Data
    public static class Plugin {
        private Map<String, String> manifest;
        private List<String> tasks;
        private List<String> triggers;
        private List<String> conditions;
        private List<String> controllers;
        private List<String> storages;

        public static Plugin of(RegisteredPlugin registeredPlugin) {
            Plugin plugin = new Plugin();

            plugin.manifest = registeredPlugin
                .getManifest()
                .getMainAttributes()
                .entrySet()
                .stream()
                .map(e -> new AbstractMap.SimpleEntry<>(
                    e.getKey().toString(),
                    e.getValue().toString()
                ))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            plugin.tasks = className(registeredPlugin.getTasks().toArray(Class[]::new));
            plugin.triggers = className(registeredPlugin.getTriggers().toArray(Class[]::new));
            plugin.conditions = className(registeredPlugin.getConditions().toArray(Class[]::new));
            plugin.controllers = className(registeredPlugin.getControllers().toArray(Class[]::new));
            plugin.storages = className(registeredPlugin.getStorages().toArray(Class[]::new));

            return plugin;
        }

        @SuppressWarnings("rawtypes")
        private static <T> List<String> className(Class[] classes) {
            return Arrays.stream(classes)
                .map(Class::getName)
                .collect(Collectors.toList());
        }
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    public static class Doc {
        String markdown;
        Schema schema;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    public static class PluginIcon {
        String name;
        String icon;
        Boolean flowable;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    public static class Schema {
        private Map<String, Object> properties;
        private Map<String, Object> outputs;
    }
}
