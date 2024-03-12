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
    <div class="editor-panel">
        <FileTree :files="files" class="filetree" />

        <Editor
            class="editor"
            :navbar="false"
            :full-height="false"
            :input="true"
            model-value="this is an editor text"
        />
    </div>
</template>

<script setup>
    import Plus from "vue-material-design-icons/Plus.vue";
    import FolderPlus from "vue-material-design-icons/FolderPlus.vue";
    import FilePlus from "vue-material-design-icons/FilePlus.vue";
    import FolderZip from "vue-material-design-icons/FolderZip.vue";
    import NamespaceSelect from "./NamespaceSelect.vue";
    import TopNavBar from "../layout/TopNavBar.vue";
    import TriggerFlow from "../flows/TriggerFlow.vue";
    import Editor from "../../components/inputs/Editor.vue";
    import FileTree from "../../components/FileTree.vue";
</script>

<script>
    import RouteContext from "../../mixins/routeContext";
    import RestoreUrl from "../../mixins/restoreUrl";
    import {apiUrl} from "override/utils/route";
    import {mapState} from "vuex";
    import {storageKeys} from "../../utils/constants";

    export default {
        mixins: [RouteContext, RestoreUrl],
        data() {
            return {
                files: () => ({}),
                flow: null
            };
        },
        mounted() {
            this.getFiles();
        },
        watch: {
            namespace() {
                this.getFiles();
            }
        },
        methods: {
            namespaceUpdate(namespace) {
                localStorage.setItem(storageKeys.LATEST_NAMESPACE, namespace);
                this.$router.push({
                    params: {
                        namespace
                    }
                });
            },
            getFiles(localPath="") {
                return fetch(`${this.apiUrl}/namespaces/${this.namespace}/files/directory?path=/${localPath}`)
                    .then((response) => {
                        return response.json()
                    }).then((response) => {
                        // TODO: this should fill the store instead
                        this.files = response;
                    });
            }
        },
        computed: {
            // TODO: add the file list to the store
            // so it can be loaded and cached
            ...mapState("namespace", ["namespaces"]),
            routeInfo() {
                // TODO: could this be the same as left menu ?
                // or at least defined once when we define routes instead of 3 times
                return {
                    title: this.$t("editor")
                };
            },
            apiUrl() {
                return apiUrl(this.$store);
            },
            namespace() {
                return this.$route.params.namespace;
            }
        },
    }
</script>

<style scoped>
.editor-panel {
    display: flex;
    flex-direction: row;
    align-items: stretch;
    width: 100%;
}

.filetree {
    width: 300px;
    border-right: 1px solid #e0e0e0;
    padding: 1rem;
}

.editor{
    padding: 1rem;
}

</style>


