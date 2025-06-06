<template>
  <el-table :data="stocks" border style="width: 100%">
    <el-table-column prop="ts_code" label="股票代码" width="150"></el-table-column>
    <el-table-column prop="name" label="股票名称"></el-table-column>
    <el-table-column prop="buy_price" label="买入价格" width="120"></el-table-column>
    <el-table-column prop="buy_date" label="买入日期" width="150"></el-table-column>
  </el-table>
</template>

<script>
export default {
  props: {
    stocks: {
      type: Array,
      required: true
    }
  }
};
</script>