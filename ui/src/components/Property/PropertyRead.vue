<script setup>
import { getPropertyByGroup } from '@/api/index'
import { watch, getCurrentInstance } from "@vue/runtime-core"
import { useRouter, useRoute } from 'vue-router';
import { ElMessage } from 'element-plus'
import { onMounted, reactive } from 'vue';
const { proxy } = getCurrentInstance()
// Loading
const route = useRoute()
const $bus = getCurrentInstance().appContext.config.globalProperties.mittBus
const $loadingCreate = getCurrentInstance().appContext.config.globalProperties.$loadingCreate
const $loadingClose = proxy.$loadingClose
const data = reactive({
    group: "",
    tableData: []
})
const getProperty = () => {
    $loadingCreate()
    getPropertyByGroup(route.params.group)
        .then(res => {
            if (res.status == 200) {
                
            }
        })
        .catch((err) => {
            ElMessage({
                message: 'An error occurred while obtaining group data. Please refresh and try again. Error: ' + err,
                type: "error",
                duration: 3000
            })
        })
        .finally(() => {
            $loadingClose()
        })
}
const openAddProperty = () => {

}
watch(() => route, () => {
    data.group = route.params.group
}, {
    deep: true,
    immediate: true
})
onMounted(() => {
    getProperty()
})
</script>
<template>
    <div>
        <el-card shadow="always">
            <template #header>
                <div class="flex">
                    <span class="text-bold">Group：</span>
                    <span style="margin-right: 20px;">{{ data.group }}</span>
                    <span class="text-bold">Operation：</span>
                    <span>Read</span>
                </div>
            </template>
            <el-button size="small" type="primary" color="#6E38F7" @click="openAddProperty">Add
                Property</el-button>
            <el-table :data="data.tableData" style="width: 100%; margin-top: 20px;" border>
                <el-table-column label="container.group" prop="containerGroup"></el-table-column>
                <el-table-column label="container.name" prop="containerName"></el-table-column>
                <el-table-column label="container.ID" prop="containerId"></el-table-column>
                <el-table-column label="container.modRevision" prop="containerModRevision"></el-table-column>
                <el-table-column label="container.createRevision" prop="containerCreateRevision"></el-table-column>
                <el-table-column label="ID" prop="containerCreateRevision"></el-table-column>
                <el-table-column label="tags.configuration" prop="containerCreateRevision"></el-table-column>
                <el-table-column label="tags.disabled" prop="containerCreateRevision"></el-table-column>
                <el-table-column label="tags.update_time" prop="containerCreateRevision"></el-table-column>
                <el-table-column label="Operator">
                    <template #default="scope">
                        <el-button link type="primary" @click.prevent="openEditField(scope.$index)"
                            style="color: var(--color-main); font-weight: bold;">Edit</el-button>
                        <el-popconfirm @confirm="deleteTableData(scope.$index)" title="Are you sure to delete this?">
                            <template #reference>
                                <el-button link type="danger" style="color: red;font-weight: bold;">Delete</el-button>
                            </template>
                        </el-popconfirm>
                    </template>
                </el-table-column>
            </el-table>
        </el-card>
    </div>
</template>
<style lang="scss" scoped>
::v-deep {
    .el-card {
        margin: 15px;
    }
}
</style>