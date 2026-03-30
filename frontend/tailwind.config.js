/** @type {import('tailwindcss').Config} */
export default {
  darkMode: 'class',
  content: [
    "./index.html",
    "./src/**/*.{vue,js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        business: {
          darker: '#0f172a',  // 深靛蓝底色
          dark: '#1e293b',    // 容器底色
          light: '#334155',   // 边框/次要色
          accent: '#3b82f6',  // 商务蓝
          highlight: '#06b6d4', // 青色点缀
          success: '#f43f5e', // A股习惯：红色涨
          danger: '#10b981',  // A股习惯：绿色跌
        }
      },
      borderRadius: {
        '2xl': '1rem',
        '3xl': '1.5rem',
      },
      boxShadow: {
        'business': '0 10px 25px -3px rgba(0, 0, 0, 0.3), 0 4px 6px -2px rgba(0, 0, 0, 0.05)',
      },
      animation: {
        'blink-orange': 'blink-orange 1.5s ease-in-out infinite',
        'blink-red': 'blink-red 1.5s ease-in-out infinite',
      },
      keyframes: {
        'blink-orange': {
          '0%, 100%': { 
            borderColor: 'rgba(251, 146, 60, 0.3)',
            backgroundColor: 'rgba(251, 146, 60, 0.08)',
          },
          '50%': { 
            borderColor: 'rgba(251, 146, 60, 0.6)',
            backgroundColor: 'rgba(251, 146, 60, 0.15)',
          },
        },
        'blink-red': {
          '0%, 100%': { 
            borderColor: 'rgba(239, 68, 68, 0.3)',
            backgroundColor: 'rgba(239, 68, 68, 0.08)',
          },
          '50%': { 
            borderColor: 'rgba(239, 68, 68, 0.6)',
            backgroundColor: 'rgba(239, 68, 68, 0.15)',
          },
        },
      },
    },
  },
  plugins: [],
}