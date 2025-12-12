import { useState, useEffect, useCallback } from 'react'
import axios from 'axios'
import IndexChart from './components/IndexChart'
import StatsCard from './components/StatsCard'
import TimeRangeSelector from './components/TimeRangeSelector'

function App() {
    const [historyData, setHistoryData] = useState([])
    const [stats, setStats] = useState(null)
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState(null)
    const [timeRange, setTimeRange] = useState(168) // 默认7天
    const [autoRefresh, setAutoRefresh] = useState(true)

    const fetchData = useCallback(async () => {
        try {
            setError(null)

            const [historyRes, statsRes] = await Promise.all([
                axios.get(`/api/index/history?hours=${timeRange}`),
                axios.get('/api/index/stats')
            ])

            if (historyRes.data.success) {
                setHistoryData(historyRes.data.data)
            }

            if (statsRes.data.success) {
                setStats(statsRes.data.stats)
            }

            setLoading(false)
        } catch (err) {
            console.error('获取数据失败:', err)
            setError(err.message || '获取数据失败')
            setLoading(false)
        }
    }, [timeRange])

    useEffect(() => {
        fetchData()
    }, [fetchData])

    // 自动刷新
    useEffect(() => {
        if (!autoRefresh) return

        const interval = setInterval(() => {
            fetchData()
        }, 60000) // 每分钟刷新

        return () => clearInterval(interval)
    }, [autoRefresh, fetchData])

    const handleRefresh = () => {
        setLoading(true)
        fetchData()
    }

    const formatPercent = (value) => {
        if (value === null || value === undefined) return '--'
        const prefix = value >= 0 ? '+' : ''
        return `${prefix}${value.toFixed(2)}%`
    }

    const getValueClass = (value) => {
        if (value === null || value === undefined) return ''
        return value >= 0 ? 'positive' : 'negative'
    }

    return (
        <div className="app">
            {/* 统计卡片 */}
            <div className="stats-container">
                <StatsCard
                    label="📈 当前指数"
                    value={formatPercent(stats?.current)}
                    valueClass={getValueClass(stats?.current)}
                    subValue={stats?.lastUpdate ? `更新于 ${new Date(stats.lastUpdate).toLocaleTimeString()}` : ''}
                />
                <StatsCard
                    label="📊 24小时变化"
                    value={formatPercent(stats?.change24h)}
                    valueClass={getValueClass(stats?.change24h)}
                    subValue={stats?.high24h !== undefined ? `高: ${formatPercent(stats.high24h)} / 低: ${formatPercent(stats.low24h)}` : ''}
                />
                <StatsCard
                    label="📆 3天变化"
                    value={formatPercent(stats?.change3d)}
                    valueClass={getValueClass(stats?.change3d)}
                    subValue={`${stats?.dataPoints3d || 0} 个数据点`}
                />
                <StatsCard
                    label="📅 7天变化"
                    value={formatPercent(stats?.change7d)}
                    valueClass={getValueClass(stats?.change7d)}
                    subValue={`${stats?.dataPoints7d || 0} 个数据点`}
                />
                <StatsCard
                    label="🪙 参与币种"
                    value={stats?.coinCount || '--'}
                    subValue="排除 BTC、ETH"
                />
            </div>

            {/* 控制栏 */}
            <div className="controls">
                <TimeRangeSelector
                    value={timeRange}
                    onChange={setTimeRange}
                />

                <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
                    <label className="auto-refresh">
                        <input
                            type="checkbox"
                            checked={autoRefresh}
                            onChange={(e) => setAutoRefresh(e.target.checked)}
                        />
                        自动刷新
                    </label>

                    <button
                        className={`refresh-btn ${loading ? 'loading' : ''}`}
                        onClick={handleRefresh}
                        disabled={loading}
                    >
                        🔄 刷新
                    </button>
                </div>
            </div>

            {/* 图表区域 */}
            <div className="chart-container">
                <div className="chart-title">
                    📉 市场指数走势
                </div>

                {loading && historyData.length === 0 ? (
                    <div className="loading-container">
                        <div className="loading-spinner"></div>
                        <p>正在加载数据...</p>
                    </div>
                ) : error ? (
                    <div className="error-container">
                        <div className="error-icon">⚠️</div>
                        <p>{error}</p>
                        <button className="retry-btn" onClick={handleRefresh}>
                            重试
                        </button>
                    </div>
                ) : historyData.length === 0 ? (
                    <div className="no-data">
                        <div className="icon">📭</div>
                        <p>暂无数据</p>
                        <p>服务启动后需要等待数据回补完成</p>
                    </div>
                ) : (
                    <IndexChart data={historyData} />
                )}
            </div>

            <footer className="footer">
                <p>数据来源: 币安合约API | 每5分钟采集一次 | {stats?.coinCount || 0} 个币种参与计算</p>
            </footer>
        </div>
    )
}

export default App
