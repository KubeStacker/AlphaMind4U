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
import SettingsMain from './pages/SettingsMain'
import ModelK from './pages/ModelK'
import Docs from './pages/Docs'

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
                    <Navigate to="/sheep-analysis" replace />
                  </Layout>
                </ProtectedRoute>
              }
            />
            <Route
              path="/sheep-analysis"
              element={
                <ProtectedRoute>
                  <Layout>
                    <Tab1 />
                  </Layout>
                </ProtectedRoute>
              }
            />
            <Route
              path="/hot-sheep"
              element={
                <ProtectedRoute>
                  <Layout>
                    <Tab2 />
                  </Layout>
                </ProtectedRoute>
              }
            />
            <Route
              path="/model-k"
              element={
                <ProtectedRoute>
                  <Layout>
                    <ModelK />
                  </Layout>
                </ProtectedRoute>
              }
            />
            <Route
              path="/settings/*"
              element={
                <ProtectedRoute>
                  <Layout>
                    <SettingsMain />
                  </Layout>
                </ProtectedRoute>
              }
            />
            <Route
              path="/docs"
              element={
                <ProtectedRoute>
                  <Layout>
                    <Docs />
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
