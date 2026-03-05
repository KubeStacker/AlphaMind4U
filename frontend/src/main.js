import { createApp } from 'vue'
import { createPinia } from 'pinia'

import App from './App.vue'
import router from './router'
import './assets/main.css'

// ECharts
import ECharts from 'vue-echarts'
import { use } from "echarts/core";
import { CanvasRenderer } from "echarts/renderers";
import { HeatmapChart, BarChart, LineChart, PieChart, CandlestickChart } from "echarts/charts";
import {
  TooltipComponent,
  VisualMapComponent,
  TitleComponent,
  CalendarComponent,
  GridComponent,
  LegendComponent,
  MarkLineComponent,
  MarkPointComponent,
  DataZoomComponent,
  ToolboxComponent
} from "echarts/components";

// 注册 ECharts 组件
use([
  CanvasRenderer,
  HeatmapChart,
  BarChart,
  LineChart,
  PieChart,
  CandlestickChart,
  CalendarComponent,
  GridComponent,
  LegendComponent,
  TooltipComponent,
  VisualMapComponent,
  TitleComponent,
  MarkLineComponent,
  MarkPointComponent,
  DataZoomComponent,
  ToolboxComponent
]);


const app = createApp(App)

app.use(createPinia())
app.use(router)

// 全局注册 echarts 组件
app.component('v-chart', ECharts)

app.mount('#app')
