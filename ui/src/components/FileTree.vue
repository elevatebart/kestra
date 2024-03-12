<script setup>
    import FileIcon from "vue-material-design-icons/File.vue";
    import Folder from "vue-material-design-icons/Folder.vue";
    import PlusIcon from "vue-material-design-icons/Plus.vue"
</script>

<script>
    export default {
        props: {
            files:{
                type: Array,
                default: () => []
            },
        },
        methods: {
            handleClick(file) {
                // expand the file in the store
                // or signal to the store which file(s) are selected
                console.log(file)
                return
            }

        }
    }
</script>

<template>
    <ul>
        <li v-for="file in files" :key="file.fileName">
            <FileIcon v-if="file.type==='File'" />
            <template v-else>
                <PlusIcon class="expand-icon" />
                <Folder />
            </template>
            <span>
                {{ file.fileName }}
            </span>
            <FileTree v-if="file.children && file.expanded" :files="file.children" />
        </li>
    </ul>
</template>

<style scoped>
ul{
    padding-left: 20px;
}

ul li{
    position: relative;
    white-space: nowrap;
    display: flex;
    gap: 0.5rem;
    align-items: center;
    padding: 0.3rem 0.5rem;
}

.expand-icon{
    position: absolute;
    left: -12px;
}
</style>
