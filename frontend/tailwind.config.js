/** @type {import('tailwindcss').Config} */
export default {
  darkMode: 'class',
  content: [
    "./index.html",
    "./src/**/*.{vue,js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      fontFamily: {
        sans: ['Geist', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'ui-monospace', 'monospace'],
      },
      colors: {
        obsidian: {
          950: '#08090d',
          900: '#0c0e14',
          800: '#12151f',
          700: '#1a1e2e',
          600: '#232840',
        },
        signal: {
          bull: '#f59e0b',
          bullDim: '#d97706',
          bullGlow: 'rgba(245, 158, 11, 0.15)',
          bear: '#ef4444',
          bearDim: '#dc2626',
          bearGlow: 'rgba(239, 68, 68, 0.12)',
          neutral: '#94a3b8',
          data: '#06b6d4',
          dataGlow: 'rgba(6, 182, 212, 0.10)',
          accent: '#6366f1',
          accentGlow: 'rgba(99, 102, 241, 0.12)',
        },
        business: {
          darker: '#0c0e14',
          dark: '#12151f',
          light: '#1a1e2e',
          accent: '#6366f1',
          highlight: '#06b6d4',
          success: '#f59e0b',
          danger: '#ef4444',
        }
      },
      borderRadius: {
        '2xl': '1rem',
        '3xl': '1.5rem',
      },
      boxShadow: {
        'business': '0 10px 25px -3px rgba(0, 0, 0, 0.4), 0 4px 6px -2px rgba(0, 0, 0, 0.08)',
        'glow-bull': '0 0 20px rgba(245, 158, 11, 0.08)',
        'glow-bear': '0 0 20px rgba(239, 68, 68, 0.08)',
        'glow-data': '0 0 20px rgba(6, 182, 212, 0.06)',
        'inner-subtle': 'inset 0 1px 0 rgba(255, 255, 255, 0.03)',
      },
      animation: {
        'blink-orange': 'blink-orange 1.5s ease-in-out infinite',
        'blink-red': 'blink-red 1.5s ease-in-out infinite',
        'fade-in-up': 'fade-in-up 0.4s ease-out both',
        'fade-in': 'fade-in 0.3s ease-out both',
        'slide-up': 'slide-up 0.35s cubic-bezier(0.16, 1, 0.3, 1) both',
        'pulse-subtle': 'pulse-subtle 2s ease-in-out infinite',
        'shimmer': 'shimmer 2s linear infinite',
      },
      keyframes: {
        'blink-orange': {
          '0%, 100%': { 
            borderColor: 'rgba(245, 158, 11, 0.25)',
            backgroundColor: 'rgba(245, 158, 11, 0.06)',
          },
          '50%': { 
            borderColor: 'rgba(245, 158, 11, 0.5)',
            backgroundColor: 'rgba(245, 158, 11, 0.12)',
          },
        },
        'blink-red': {
          '0%, 100%': { 
            borderColor: 'rgba(239, 68, 68, 0.25)',
            backgroundColor: 'rgba(239, 68, 68, 0.06)',
          },
          '50%': { 
            borderColor: 'rgba(239, 68, 68, 0.5)',
            backgroundColor: 'rgba(239, 68, 68, 0.12)',
          },
        },
        'fade-in-up': {
          from: { opacity: '0', transform: 'translateY(8px)' },
          to: { opacity: '1', transform: 'translateY(0)' },
        },
        'fade-in': {
          from: { opacity: '0' },
          to: { opacity: '1' },
        },
        'slide-up': {
          from: { opacity: '0', transform: 'translateY(12px)' },
          to: { opacity: '1', transform: 'translateY(0)' },
        },
        'pulse-subtle': {
          '0%, 100%': { opacity: '1' },
          '50%': { opacity: '0.7' },
        },
        'shimmer': {
          '0%': { backgroundPosition: '-200% 0' },
          '100%': { backgroundPosition: '200% 0' },
        },
      },
      backgroundImage: {
        'noise': "url(\"data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)' opacity='0.03'/%3E%3C/svg%3E\")",
        'shimmer': 'linear-gradient(90deg, transparent, rgba(255,255,255,0.03), transparent)',
      },
    },
  },
  plugins: [],
}
