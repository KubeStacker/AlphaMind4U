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
 * 获取策略推荐
 * @param {Object} params - 包含 target_date, concept 等
 */
export const getRecommendations = (params = {}) => {
  return apiClient.get('/strategy/recommendations', { params });
};

/** 获取热门概念 */
export const getHotConcepts = () => apiClient.get('/strategy/hot_concepts');

/** 获取主线演变历史 */
export const getMainlineHistory = (days = 30) => apiClient.get('/strategy/mainline_history', { params: { days } });

/** 获取市场情绪历史 */
export const getMarketSentiment = (days = 30) => apiClient.get('/strategy/market_sentiment', { params: { days } });

/** 触发收益验证 */
export const triggerBacktest = () => apiClient.post('/strategy/backtest');

/** 获取验证结果 */
export const getBacktestResults = (date) => apiClient.get('/strategy/backtest/results', { params: { date } });

/** 获取科创50回测结果 */
export const getStar50Backtest = () => apiClient.get('/strategy/backtest/star50');

/**
 * 获取后端状态 (此端点需在后端实现)
 *
 * @returns {Promise<Object>} e.g., { status: 'TRADING' | 'CLOSED' }
 */
export const getSystemStatus = () => {
    // 假设后端提供了一个 /status 接口来返回当前市场状态
    return apiClient.get('/system/status');
};

// --- 用户管理 ---
export const listUsers = () => apiClient.get('/admin/users');
export const createUser = (userData) => apiClient.post('/admin/users', userData);
export const deleteUser = (userId) => apiClient.delete(`/admin/users/${userId}`);
export const updatePassword = (userId, newPassword) => apiClient.put('/admin/users/password', { user_id: userId, new_password: newPassword });

// --- 数据库查询 ---
export const executeDBQuery = (sql) => apiClient.post('/admin/db/query', { sql });

// ... 此处可以添加更多 API 调用
