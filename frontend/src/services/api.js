// src/services/api.js
import axios from 'axios';
import { useAuthStore } from '@/stores/auth';

// 创建一个 Axios 实例
const apiClient = axios.create({
  baseURL: '/api', // 所有请求都将以 /api 开头，由 Vite 代理
});

// 添加一个请求拦截器，在每个请求中附加 Token
apiClient.interceptors.request.use(
  (config) => {
    const authStore = useAuthStore();
    const token = authStore.token;
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// --- 封装的 API 调用 ---

/**
 * 获取数据完整性报告
 * @param {string} startDate 
 * @param {string} endDate 
 */
export const getIntegrityReport = (startDate, endDate) => {
  return apiClient.get('/admin/integrity', { params: { start_date: startDate, end_date: endDate } });
};

/**
 * 获取后端状态
 * @returns {Promise<Object>} e.g., { status: 'TRADING' | 'CLOSED' }
 */
export const getSystemStatus = () => {
    return apiClient.get('/system/status');
};

/** 触发特定日期的行情同步 */
const triggerSyncTask = (payload) => apiClient.post('/admin/etl/sync', payload);

/** 触发特定日期的行情同步 */
export const syncDailyDate = (date) => {
  if (date) {
    return triggerSyncTask({ task: 'daily', start_date: date, end_date: date, years: 0, calc_factors: true });
  }
  return triggerSyncTask({ task: 'daily', years: 1, calc_factors: true });
};

/** 同步财务指标 */
export const syncFinancials = (limit = 1000) => triggerSyncTask({ task: 'financials', limit });

/** 同步财务指标(新) */
export const syncFinaIndicator = (limit = 500) => triggerSyncTask({ task: 'fina_indicator', limit });

/** 同步季度利润表 */
export const syncQuarterlyIncome = (tsCode = null, startYear = 2020) => triggerSyncTask({ task: 'quarterly_income', ts_code: tsCode });

/** 同步资金流向 */
export const syncMoneyflow = (years = 1) => triggerSyncTask({ task: 'moneyflow', years });

/** 同步市场指数 */
export const syncMarketIndex = (tsCode = '000001.SH', years = 1) => triggerSyncTask({ task: 'index', ts_code: tsCode, years });

/** 同步融资融券数据 */
export const syncMargin = (days = 90) => triggerSyncTask({ task: 'margin', days });

/** 获取后台任务执行状态 */
export const getTasksStatus = () => apiClient.get('/admin/tasks/status');

// --- 用户管理 ---
export const listUsers = () => apiClient.get('/admin/users');
export const createUser = (userData) => apiClient.post('/admin/users', userData);
export const deleteUser = (userId) => apiClient.delete(`/admin/users/${userId}`);
export const updatePassword = (userId, newPassword) => apiClient.put('/admin/users/password', { user_id: userId, new_password: newPassword });

// --- 数据库查询 ---
export const executeDBQuery = (sql) => apiClient.post('/admin/db/query', { sql });

// --- 数据统计 ---
export const getTableStats = () => apiClient.post('/admin/db/query', {
  sql: `SELECT COUNT(*) as cnt, 'stock_income' as tbl FROM stock_income 
        UNION ALL SELECT COUNT(*), 'stock_fina_indicator' FROM stock_fina_indicator 
        UNION ALL SELECT COUNT(*), 'stock_basic' FROM stock_basic
        UNION ALL SELECT COUNT(*), 'daily_price' FROM daily_price
        UNION ALL SELECT COUNT(*), 'stock_moneyflow' FROM stock_moneyflow`
});

// --- 市场数据 ---
export const getMarketSentiment = (days = 30) => apiClient.get('/admin/market_sentiment', { params: { days } });
export const getSentimentPreview = (src = 'dc') => apiClient.get('/admin/sentiment/preview', { params: { src } });
export const getMarketSuggestion = (params = {}) => apiClient.get('/admin/market/suggestion', { params });
export const syncSentiment = (days = 250, syncIndex = false) =>
  apiClient.post('/admin/etl/sentiment', null, { params: { days, sync_index: syncIndex } });
export const getBacktestResult = (optimize = true) => apiClient.get('/admin/backtest_result', { params: { optimize } });
export const getMainlineHistory = (days = 30) => apiClient.get('/admin/mainline_history', { params: { days } });
export const getMarginHeatmap = (days = 10, top_n = 30) => apiClient.get('/admin/margin_heatmap', { params: { days, top_n } });

/** 校验数据准确性 */
export const verifyDataAccuracy = (tsCode = "688256.SH") => apiClient.get('/admin/data_verify', { params: { ts_code: tsCode } });
