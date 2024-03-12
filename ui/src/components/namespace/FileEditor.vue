<template>
    <top-nav-bar :title="routeInfo.title">
        <template #additional-right>
            <namespace-select
                class="fit-content"
                data-type="flow"
                :value="namespace"
                @update:model-value="namespaceUpdate"
                allow-create
                :is-filter="false"
            />

            <el-dropdown>
                <el-button :icon="Plus" class="p-2 m-0" />
                <template #dropdown>
                    <el-dropdown-menu>
                        <el-dropdown-item :icon="FilePlus" @click="pickFile">
                            <input
                                ref="filePicker"
                                type="file"
                                multiple
                                style="display: none"
                                @change="importNsFiles"
                            >
                            {{ $t("namespace files.import.file") }}
                        </el-dropdown-item>
                        <el-dropdown-item :icon="FolderPlus" @click="pickFolder">
                            <input
                                ref="folderPicker"
                                type="file"
                                webkitdirectory
                                mozdirectory
                                msdirectory
                                odirectory
                                directory
                                style="display: none"
                                @change="importNsFiles"
                            >
                            {{ $t("namespace files.import.folder") }}
                        </el-dropdown-item>
                    </el-dropdown-menu>
                </template>
            </el-dropdown>
            <el-tooltip :hide-after="50" :content="$t('namespace files.export')">
                <el-button :icon="FolderZip" class="p-2 m-0" @click="exportNsFiles" />
            </el-tooltip>
            <trigger-flow
                ref="triggerFlow"
                :disabled="!flow"
                :flow-id="flow"
                :namespace="namespace"
            />
        </template>
    </top-nav-bar>
    <div>New Editor with FileTree</div>
</template>

<script setup>
    import NamespaceSelect from "./NamespaceSelect.vue";
    import TopNavBar from "../layout/TopNavBar.vue";
    import TriggerFlow from "../flows/TriggerFlow.vue";
    import Plus from "vue-material-design-icons/Plus.vue";
    import FolderPlus from "vue-material-design-icons/FolderPlus.vue";
    import FilePlus from "vue-material-design-icons/FilePlus.vue";
    import FolderZip from "vue-material-design-icons/FolderZip.vue";
</script>

<script>
    import {apiUrl} from "override/utils/route";
    import {mapState} from "vuex";

    export default {
        data() {
            return {

                flow: null,
            };
        },
        computed: {
            ...mapState("namespace", ["namespaces"]),
            routeInfo() {
                return {
                    title: this.$t("editor")
                };
            },
            theme() {
                return localStorage.getItem("theme") || "light";
            },
            vscodeIndexUrl() {
                const uiSubpath = KESTRA_UI_PATH === "./" ? "/" : KESTRA_UI_PATH;
                return `${uiSubpath}vscode.html?KESTRA_UI_PATH=${uiSubpath}&KESTRA_API_URL=${apiUrl(this.$store)}&THEME=${this.theme}&namespace=${this.namespace}`;
            },
            namespace() {
                return this.$route.params.namespace;
            }
        },
    }
</script>


