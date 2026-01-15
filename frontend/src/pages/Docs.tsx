import React, { useState, useEffect } from 'react'
import { Card, Spin, message } from 'antd'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import client from '../api/client'

const Docs: React.FC = () => {
  const [content, setContent] = useState<string>('')
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const loadDocs = async () => {
      try {
        setLoading(true)
        
        // 优先从API加载文档
        try {
          const response = await client.get('/docs/model-k-usage')
          if (response.data && response.data.content) {
            setContent(response.data.content)
            return
          }
        } catch (apiError: any) {
          console.warn('API加载文档失败，尝试静态文件:', apiError?.message || apiError)
          // API失败，继续尝试静态文件
        }
        
        // 如果API失败，尝试从静态文件加载
        try {
          const response = await fetch('/docs/model-k-usage.md')
          if (response.ok) {
            const text = await response.text()
            setContent(text)
            return
          }
        } catch (staticError) {
          console.warn('静态文件加载失败:', staticError)
        }
        
        // 如果都失败了，显示默认内容
        throw new Error('文档加载失败')
      } catch (error: any) {
        console.error('加载文档失败:', error)
        message.error('文档加载失败，请稍后重试')
        // 显示默认内容
        setContent(`# 模型老K 使用指南

## 📖 简介

模型老K是一个基于T7概念资金双驱模型的智能选股推荐系统，采用概念竞速引擎 + 资金流验证 + 动态龙头筛选 + 筹码获利盘分析。

## ⚠️ 文档加载失败

文档暂时无法加载，请稍后重试。

如果问题持续存在，请联系系统管理员。`)
      } finally {
        setLoading(false)
      }
    }

    loadDocs()
  }, [])

  return (
    <div style={{ maxWidth: '1200px', margin: '0 auto' }}>
      <Card>
        <Spin spinning={loading}>
          <div
            style={{
              padding: '24px',
              fontSize: '16px',
              lineHeight: '1.8',
            }}
          >
            <ReactMarkdown remarkPlugins={[remarkGfm]}>{content}</ReactMarkdown>
          </div>
        </Spin>
      </Card>
    </div>
  )
}

export default Docs
