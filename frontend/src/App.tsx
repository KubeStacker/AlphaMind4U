import React from 'react'
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { ConfigProvider } from 'antd'
import zhCN from 'antd/locale/zh_CN'
import { AuthProvider } from './contexts/AuthContext'
import Layout from './components/Layout'
import ProtectedRoute from './components/ProtectedRoute'
import Login from './pages/Login'
import Tab1 from './pages/Tab1'
import Tab2 from './pages/Tab2'
import Tab3 from './pages/Tab3'

const App: React.FC = () => {
  return (
    <ConfigProvider locale={zhCN}>
      <AuthProvider>
        <BrowserRouter>
          <Routes>
            <Route path="/login" element={<Login />} />
            <Route
              path="/"
              element={
                <ProtectedRoute>
                  <Layout>
                    <Navigate to="/recommendations" replace />
                  </Layout>
                </ProtectedRoute>
              }
            />
            <Route
              path="/recommendations"
              element={
                <ProtectedRoute>
                  <Layout>
                    <Tab3 />
                  </Layout>
                </ProtectedRoute>
              }
            />
            <Route
              path="/stock-analysis"
              element={
                <ProtectedRoute>
                  <Layout>
                    <Tab1 />
                  </Layout>
                </ProtectedRoute>
              }
            />
            <Route
              path="/hot-stocks"
              element={
                <ProtectedRoute>
                  <Layout>
                    <Tab2 />
                  </Layout>
                </ProtectedRoute>
              }
            />
          </Routes>
        </BrowserRouter>
      </AuthProvider>
    </ConfigProvider>
  )
}

export default App
