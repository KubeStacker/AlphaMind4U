import { ref, onMounted } from 'vue'
import { searchStocks } from '@/services/api'

// 全局缓存，避免重复加载
let stockCache = null
let cachePromise = null

export function useStockSearch() {
  const searchResults = ref([])
  const searchLoading = ref(false)
  const showSearchResults = ref(false)
  const cacheLoaded = ref(false)

  // 加载所有股票基础数据到本地缓存
  const loadStockCache = async () => {
    if (stockCache) {
      cacheLoaded.value = true
      return
    }
    
    if (cachePromise) {
      await cachePromise
      cacheLoaded.value = true
      return
    }

    cachePromise = (async () => {
      try {
        // 通过API获取所有股票基础数据
        const res = await searchStocks('', 5000)
        stockCache = (res.data?.data || []).map(stock => ({
          ts_code: stock.ts_code,
          name: stock.name,
          // 预处理搜索字段
          ts_code_upper: stock.ts_code.toUpperCase(),
          name_upper: (stock.name || '').toUpperCase(),
          symbol: stock.ts_code.split('.')[0], // 提取纯代码部分
          pinyin: (stock.pinyin || '').toLowerCase(),
          pinyin_abbr: (stock.pinyin_abbr || '').toLowerCase()
        }))
        console.log(`股票缓存已加载: ${stockCache.length} 条记录`)
        cacheLoaded.value = true
      } catch (e) {
        console.error('加载股票缓存失败:', e)
        stockCache = []
      }
    })()

    await cachePromise
  }

  // 本地搜索函数
  const localSearch = (query, limit = 8) => {
    if (!stockCache || !query || query.length < 1) {
      return []
    }

    const q = query.trim()
    const qUpper = q.toUpperCase()
    const qLower = q.toLowerCase()
    const isDigit = /^\d+$/.test(q)
    const isChinese = /[\u4e00-\u9fff]/.test(q)

    // 过滤匹配的股票
    let filtered = stockCache.filter(stock => {
      if (isDigit) {
        // 纯数字：匹配代码
        return stock.symbol.startsWith(q) || stock.ts_code_upper.startsWith(qUpper)
      } else if (isChinese) {
        // 中文：匹配名称
        return stock.name.includes(q) || stock.name_upper.includes(q)
      } else {
        // 英文：匹配代码或拼音首字母
        return stock.ts_code_upper.includes(qUpper) || 
               stock.pinyin_abbr.includes(qLower) ||
               stock.pinyin.includes(qLower)
      }
    })

    // 排序：优先显示完全匹配，然后前缀匹配，最后包含匹配
    filtered.sort((a, b) => {
      if (isDigit) {
        // 纯数字时的排序
        const aSymbolExact = a.symbol === q ? 0 : 1
        const bSymbolExact = b.symbol === q ? 0 : 1
        if (aSymbolExact !== bSymbolExact) return aSymbolExact - bSymbolExact
        
        const aSymbolPrefix = a.symbol.startsWith(q) ? 0 : 1
        const bSymbolPrefix = b.symbol.startsWith(q) ? 0 : 1
        if (aSymbolPrefix !== bSymbolPrefix) return aSymbolPrefix - bSymbolPrefix
      } else if (isChinese) {
        // 中文时的排序
        const aNameExact = a.name === q ? 0 : 1
        const bNameExact = b.name === q ? 0 : 1
        if (aNameExact !== bNameExact) return aNameExact - bNameExact

        const aNamePrefix = a.name.startsWith(q) ? 0 : 1
        const bNamePrefix = b.name.startsWith(q) ? 0 : 1
        if (aNamePrefix !== bNamePrefix) return aNamePrefix - bNamePrefix
      } else {
        // 英文/拼音时的排序
        const aCodeExact = a.ts_code_upper === qUpper ? 0 : 1
        const bCodeExact = b.ts_code_upper === qUpper ? 0 : 1
        if (aCodeExact !== bCodeExact) return aCodeExact - bCodeExact

        const aCodePrefix = a.ts_code_upper.startsWith(qUpper) ? 0 : 1
        const bCodePrefix = b.ts_code_upper.startsWith(qUpper) ? 0 : 1
        if (aCodePrefix !== bCodePrefix) return aCodePrefix - bCodePrefix

        const aAbbrPrefix = a.pinyin_abbr.startsWith(qLower) ? 0 : 1
        const bAbbrPrefix = b.pinyin_abbr.startsWith(qLower) ? 0 : 1
        if (aAbbrPrefix !== bAbbrPrefix) return aAbbrPrefix - bAbbrPrefix

        const aAbbrContains = a.pinyin_abbr.includes(qLower) ? 0 : 1
        const bAbbrContains = b.pinyin_abbr.includes(qLower) ? 0 : 1
        if (aAbbrContains !== bAbbrContains) return aAbbrContains - bAbbrContains
      }
      
      // 最后按代码排序
      return a.ts_code.localeCompare(b.ts_code)
    })

    return filtered.slice(0, limit).map(stock => ({
      ts_code: stock.ts_code,
      name: stock.name
    }))
  }

  // 搜索函数（优先本地，降级到远程）
  const search = async (query, limit = 8) => {
    if (!query || query.length < 1) {
      searchResults.value = []
      showSearchResults.value = false
      return
    }

    // 如果缓存已加载，使用本地搜索
    if (cacheLoaded.value && stockCache) {
      searchResults.value = localSearch(query, limit)
      showSearchResults.value = searchResults.value.length > 0
      return
    }

    // 缓存未加载，使用远程搜索
    searchLoading.value = true
    try {
      const res = await searchStocks(query, limit)
      searchResults.value = res.data?.data || []
      showSearchResults.value = searchResults.value.length > 0
    } catch (e) {
      console.error('搜索失败:', e)
      searchResults.value = []
    } finally {
      searchLoading.value = false
    }
  }

  // 隐藏搜索结果
  const hideResults = () => {
    setTimeout(() => {
      showSearchResults.value = false
    }, 200)
  }

  // 清空搜索结果
  const clearResults = () => {
    searchResults.value = []
    showSearchResults.value = false
  }

  // 组件挂载时加载缓存
  onMounted(() => {
    loadStockCache()
  })

  return {
    searchResults,
    searchLoading,
    showSearchResults,
    cacheLoaded,
    search,
    hideResults,
    clearResults,
    loadStockCache
  }
}
