<template>
    <div class="container">
        <div class="image">
            <el-image :src="userImg" fit="fill">
                <div slot="error" class="image-slot">
                    <i class="el-icon-picture-outline"></i>
                </div>
            </el-image>
            <h2>BanyanDB Manager</h2>
        </div>
        <div>
            <el-menu :default-active="activeIndex" active-text-color="#6E38F7" router class="el-menu-demo" mode="horizontal" @select="handleSelect">
                <el-menu-item index="/home">Home</el-menu-item>
                <el-menu-item index="/database">Database</el-menu-item>
                <el-menu-item index="/structure">Structure</el-menu-item>
                <el-menu-item index="/about">About Us</el-menu-item>
            </el-menu>
        </div>
        <div>

        </div>
    </div>
</template>

<script>
export default {
    name: 'headerComponent',
    data() {
        return {
            userImg: require('../../../assets/banyandb_small.jpg')
        }
    },
}
</script>

<style lang="scss">
.container {
    display: flex;
    align-items: center;
    justify-content: space-between;

    .image {
        width: 250px;
        height: 59px;
        display: flex;
        align-items: center;
        .el-image {
            width: 59px;
            height: 59px;
            display: flex;
            align-items: center;
            justify-content: center;
        }

        h2 {
            height: 100%;
            line-height: 59px;
            font-size: 15px;
            font-family:'Lucida Sans', 'Lucida Sans Regular', 'Lucida Grande', 'Lucida Sans Unicode', Geneva, Verdana, sans-serif;
        }
    }
}
.el-menu-item{
    font-weight: 500;
    color: #000000;
}
</style>