<template>
  <div id="app">
    <h1>股票购买信息录入</h1>
    <StockPurchaseForm />

    <h2>当前持仓</h2>
    <HoldingStocksTable :stocks="holdingStocks" />
  </div>
</template>

<script>
import StockPurchaseForm from './components/StockPurchaseForm.vue';
import HoldingStocksTable from './components/HoldingStocksTable.vue';  // 导入新组件

export default {
  components: {
    StockPurchaseForm,
    HoldingStocksTable  // 注册组件
  },
  data() {
    return {
      holdingStocks: []
    };
  },
  mounted() {
    this.fetchHoldingStocks();
  },
  methods: {
    async fetchHoldingStocks() {
      try {
        const response = await fetch('http://localhost:5001/api/stocks');
        if (!response.ok) {
          throw new Error('网络响应失败');
        }
        const stocks = await response.json();
        this.holdingStocks = stocks.filter(stock => stock.status === 'holding');
      } catch (error) {
        console.error('获取持仓股票失败:', error);
        this.$message.error('获取持仓股票失败');
      }
    }
  }
};
</script>

<style>
#app {
  font-family: Avenir, Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
}
</style>