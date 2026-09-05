import {
  createIcons,
  Activity,
  AlertCircle,
  AlertTriangle,
  ArrowLeft,
  BookOpen,
  BookX,
  Braces,
  CheckCircle,
  Database,
  Info,
  Key,
  LogIn,
  LogOut,
  Play,
  Plug,
  RefreshCw,
  Settings,
  Table,
  TrendingUp,
  User,
  UserPlus,
  Users,
} from 'lucide'

// 按需图标集合（只打包模板实际使用的 22 个，不引全量 icons）。
const usedIcons = {
  Activity,
  AlertCircle,
  AlertTriangle,
  ArrowLeft,
  BookOpen,
  BookX,
  Braces,
  CheckCircle,
  Database,
  Info,
  Key,
  LogIn,
  LogOut,
  Play,
  Plug,
  RefreshCw,
  Settings,
  Table,
  TrendingUp,
  User,
  UserPlus,
  Users,
}

// 刷新页面内 lucide 图标（动态内容渲染后调用）。
export function refreshIcons(): void {
  createIcons({ icons: usedIcons })
}
