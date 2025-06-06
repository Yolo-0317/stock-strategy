<template>
  <el-form :model="form" label-width="120px" @submit.prevent="submitForm">
    <el-form-item label="股票代码">
      <el-input v-model="form.ts_code" required></el-input>
    </el-form-item>
    <el-form-item label="股票名称">
      <el-input v-model="form.name"></el-input>
    </el-form-item>
    <el-form-item label="买入价格">
      <el-input-number v-model="form.buy_price" :precision="4" :step="0.0001" required></el-input-number>
    </el-form-item>
    <el-form-item label="买入日期">
      <el-date-picker v-model="form.buy_date" type="date" placeholder="选择日期" required></el-date-picker>
    </el-form-item>
    <el-form-item>
      <el-button type="primary" native-type="submit">提交</el-button>
    </el-form-item>
  </el-form>
</template>

<script>
export default {
  data() {
    return {
      form: {
        ts_code: '',
        name: '',
        buy_price: 0,
        buy_date: ''
      }
    };
  },
  methods: {
    async submitForm() {
      try {
        // 格式化日期为 YYYY-MM-DD
        const formattedDate = this.form.buy_date ? new Date(this.form.buy_date).toISOString().split('T')[0] : '';
        
        const response = await fetch('http://localhost:5001/api/stocks', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            ...this.form,
            buy_date: formattedDate
          })
        });

        if (!response.ok) {
          throw new Error('网络响应失败');
        }

        const result = await response.json();
        console.log('成功:', result);
        this.$message.success('股票购买信息已保存');
      } catch (error) {
        console.error('错误:', error);
        this.$message.error('保存失败');
      }
    }
  }
};
</script>