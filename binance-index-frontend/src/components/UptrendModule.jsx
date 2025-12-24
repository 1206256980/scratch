import { useState, useEffect, useCallback, useRef } from 'react'
import axios from 'axios'
import ReactECharts from 'echarts-for-react'

// 时间选项配置
const TIME_OPTIONS = [
    { label: '1小时', value: 1 },
    { label: '4小时', value: 4 },
    { label: '12小时', value: 12 },
    { label: '1天', value: 24 },
    { label: '3天', value: 72 },
    { label: '7天', value: 168 },
    { label: '15天', value: 360 },
    { label: '30天', value: 720 },
    { label: '60天', value: 1440 }
]

// 格式化时间戳为本地时间
const formatTimestamp = (ts) => {
    if (!ts) return '--'
    const date = new Date(ts)
    const pad = (n) => String(n).padStart(2, '0')
    return `${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}`
}

function UptrendModule() {
    const [timeBase, setTimeBase] = useState(24) // 默认24小时
    const [useCustomTime, setUseCustomTime] = useState(false) // 是否使用自定义时间
    const [startTime, setStartTime] = useState('') // 开始时间
    const [endTime, setEndTime] = useState('') // 结束时间
    const [keepRatio, setKeepRatio] = useState(0.75) // 默认0.75（保留75%涨幅）
    const [inputKeepRatio, setInputKeepRatio] = useState('75') // 输入框值（以%显示）
    const [noNewHighCandles, setNoNewHighCandles] = useState(6) // 默认6根K线
    const [inputNoNewHighCandles, setInputNoNewHighCandles] = useState('6')
    const [minUptrend, setMinUptrend] = useState(4) // 默认4%
    const [inputMinUptrend, setInputMinUptrend] = useState('4') // 输入框值
    const [uptrendData, setUptrendData] = useState(null)
    const [loading, setLoading] = useState(false)
    const [selectedBucket, setSelectedBucket] = useState(null) // 选中的区间
    const [showAllRanking, setShowAllRanking] = useState(false) // 显示全部排行榜
    const [copiedSymbol, setCopiedSymbol] = useState(null) // 复制提示
    const [sortOrder, setSortOrder] = useState('desc') // 排序方向
    const [sortBy, setSortBy] = useState('uptrend') // 排序类型: 'uptrend' 或 'startTime'
    const [filterOngoing, setFilterOngoing] = useState(false) // 只看进行中
    const [selectedSymbol, setSelectedSymbol] = useState(null) // 选中的币种（查看详情）
    const [searchSymbol, setSearchSymbol] = useState('') // 搜索币种
    const chartRef = useRef(null)

    // 获取数据
    const fetchData = useCallback(async () => {
        setLoading(true)
        setSelectedBucket(null)
        setShowAllRanking(false)
        setSelectedSymbol(null)
        try {
            let url = `/api/index/uptrend-distribution?keepRatio=${keepRatio}&noNewHighCandles=${noNewHighCandles}&minUptrend=${minUptrend}`
            if (useCustomTime && startTime && endTime) {
                // datetime-local格式: 2024-12-23T16:00 → API格式: 2024-12-23 16:00
                const apiStart = startTime.replace('T', ' ')
                const apiEnd = endTime.replace('T', ' ')
                url += `&start=${encodeURIComponent(apiStart)}&end=${encodeURIComponent(apiEnd)}`
            } else {
                url += `&hours=${timeBase}`
            }
            const res = await axios.get(url)
            if (res.data.success) {
                setUptrendData(res.data.data)
            } else {
                console.error('获取单边涨幅数据失败:', res.data.message)
            }
        } catch (err) {
            console.error('获取单边涨幅数据失败:', err)
        }
        setLoading(false)
    }, [timeBase, keepRatio, noNewHighCandles, minUptrend, useCustomTime, startTime, endTime])

    useEffect(() => {
        fetchData()
    }, [fetchData])

    // 处理保留比率输入（用户输入75表示75%，内部存储0.75）
    const handleKeepRatioChange = (e) => {
        setInputKeepRatio(e.target.value)
    }

    const applyKeepRatio = () => {
        const val = parseFloat(inputKeepRatio)
        if (!isNaN(val) && val > 0 && val <= 100) {
            setKeepRatio(val / 100) // 转换为0-1范围
        } else {
            setInputKeepRatio(String(Math.round(keepRatio * 100)))
        }
    }

    const handleKeepRatioKeyDown = (e) => {
        if (e.key === 'Enter') applyKeepRatio()
    }

    // 处理横盘K线数输入
    const handleNoNewHighCandlesChange = (e) => {
        setInputNoNewHighCandles(e.target.value)
    }

    const applyNoNewHighCandles = () => {
        const val = parseInt(inputNoNewHighCandles)
        if (!isNaN(val) && val >= 1 && val <= 100) {
            setNoNewHighCandles(val)
        } else {
            setInputNoNewHighCandles(String(noNewHighCandles))
        }
    }

    const handleNoNewHighCandlesKeyDown = (e) => {
        if (e.key === 'Enter') applyNoNewHighCandles()
    }

    // 处理最小涨幅输入
    const handleMinUptrendChange = (e) => {
        setInputMinUptrend(e.target.value)
    }

    // 应用最小涨幅
    const applyMinUptrend = () => {
        const val = parseFloat(inputMinUptrend)
        if (!isNaN(val) && val >= 0 && val <= 50) {
            setMinUptrend(val)
        } else {
            setInputMinUptrend(String(minUptrend))
        }
    }

    // 回车应用最小涨幅
    const handleMinUptrendKeyDown = (e) => {
        if (e.key === 'Enter') {
            applyMinUptrend()
        }
    }

    // 查看单个币种的所有波段
    const handleViewSymbolWaves = (symbol) => {
        setSelectedSymbol(symbol)
        setSelectedBucket(null)
        setShowAllRanking(false)
    }

    // 复制币种名称
    const handleCopySymbol = async (symbol) => {
        try {
            if (navigator.clipboard && window.isSecureContext) {
                await navigator.clipboard.writeText(symbol)
            } else {
                const textArea = document.createElement('textarea')
                textArea.value = symbol
                textArea.style.position = 'fixed'
                textArea.style.left = '-9999px'
                document.body.appendChild(textArea)
                textArea.select()
                document.execCommand('copy')
                document.body.removeChild(textArea)
            }
            setCopiedSymbol(symbol)
            setTimeout(() => setCopiedSymbol(null), 1500)
        } catch (err) {
            console.error('复制失败:', err)
        }
    }

    // 关闭面板
    const closePanel = () => {
        setSelectedBucket(null)
        setShowAllRanking(false)
        setSelectedSymbol(null)
    }

    // 搜索币种
    const handleSearchSymbol = () => {
        const keyword = searchSymbol.trim().toUpperCase()
        if (!keyword) return

        if (uptrendData?.allCoinsRanking) {
            // 模糊匹配：搜索包含关键词的币种
            const matches = uptrendData.allCoinsRanking.filter(c => c.symbol.includes(keyword))

            if (matches.length > 0) {
                // 优先精确匹配，否则取第一个匹配结果
                const exactMatch = matches.find(c => c.symbol === keyword || c.symbol === keyword + 'USDT')
                const targetSymbol = exactMatch ? exactMatch.symbol : matches[0].symbol

                setSelectedSymbol(targetSymbol)
                setSelectedBucket(null)
                setShowAllRanking(false)
            } else {
                alert(`未找到包含 "${keyword}" 的币种`)
            }
        }
    }

    const handleSearchKeyDown = (e) => {
        if (e.key === 'Enter') handleSearchSymbol()
    }

    // 图表点击事件
    const onChartClick = (params) => {
        if (!uptrendData || !uptrendData.distribution) return

        let dataIndex = params.dataIndex
        if (params.componentType === 'xAxis') {
            dataIndex = params.dataIndex
        }

        if (dataIndex !== undefined && dataIndex !== null) {
            const bucket = uptrendData.distribution[dataIndex]
            if (bucket && bucket.count > 0) {
                setShowAllRanking(false)
                setSelectedBucket(bucket)
            }
        }
    }

    // 显示全部排行榜
    const handleShowAllRanking = () => {
        setSelectedBucket(null)
        setShowAllRanking(true)
    }

    // 直方图配置
    const getHistogramOption = () => {
        if (!uptrendData || !uptrendData.distribution) {
            return {}
        }

        const distribution = uptrendData.distribution
        const ranges = distribution.map(d => d.range)
        const counts = distribution.map(d => d.count)
        const ongoingCounts = distribution.map(d => d.ongoingCount || 0)

        return {
            backgroundColor: 'transparent',
            tooltip: {
                trigger: 'axis',
                axisPointer: { type: 'shadow' },
                backgroundColor: 'rgba(22, 27, 34, 0.95)',
                borderColor: 'rgba(239, 68, 68, 0.3)',
                textStyle: { color: '#f1f5f9' },
                confine: true,
                formatter: function (params) {
                    if (!params || params.length === 0) return ''
                    const param = params[0]
                    const bucket = distribution[param.dataIndex]
                    if (!bucket || bucket.count === 0) {
                        return `<div style="padding: 8px;">
                            <div style="font-weight: 600;">${bucket.range}</div>
                            <div style="color: #94a3b8;">该区间暂无币种</div>
                        </div>`
                    }
                    let html = `<div style="padding: 8px; max-width: 320px;">
                        <div style="font-weight: 600; margin-bottom: 8px; color: #ef4444;">📈 ${bucket.range}</div>
                        <div>币种数量: <span style="color: #ef4444; font-weight: 600;">${bucket.count}</span></div>
                        <div>进行中: <span style="color: #f59e0b; font-weight: 600;">${bucket.ongoingCount || 0}</span></div>`
                    if (bucket.coins && bucket.coins.length > 0) {
                        const displayCoins = bucket.coins.slice(0, 10)
                        const moreCount = bucket.coins.length - 10
                        let coinsHtml = '<div style="margin-top: 6px; font-size: 11px; color: #94a3b8;">'
                        displayCoins.forEach(coin => {
                            const ongoingMark = coin.ongoing ? '🔴' : ''
                            coinsHtml += `<div style="margin: 2px 0;">${coin.symbol} ${ongoingMark} +${coin.uptrendPercent.toFixed(1)}%</div>`
                        })
                        if (moreCount > 0) {
                            coinsHtml += `<div style="margin-top: 4px; color: #64748b;">等 ${moreCount} 个...</div>`
                        }
                        coinsHtml += '</div>'
                        html += coinsHtml
                    }
                    html += '<div style="font-size: 11px; color: #ef4444; margin-top: 6px; font-weight: 500;">👆 点击查看完整排行</div>'
                    html += '</div>'
                    return html
                }
            },
            legend: {
                show: true,
                data: ['总数', '进行中'],
                textStyle: { color: '#94a3b8', fontSize: 11 },
                top: 5,
                right: 10
            },
            grid: {
                left: '3%',
                right: '4%',
                top: '15%',
                bottom: '15%',
                containLabel: true
            },
            xAxis: {
                type: 'category',
                data: ranges,
                axisLabel: {
                    color: '#64748b',
                    rotate: 0,
                    fontSize: 11
                },
                axisLine: { lineStyle: { color: 'rgba(100, 116, 139, 0.2)' } },
                triggerEvent: true
            },
            yAxis: {
                type: 'value',
                name: '币种数',
                nameTextStyle: { color: '#64748b' },
                axisLabel: { color: '#64748b' },
                splitLine: { lineStyle: { color: 'rgba(100, 116, 139, 0.1)' } }
            },
            series: [
                {
                    name: '总数',
                    type: 'bar',
                    data: counts.map((count, index) => ({
                        value: count === 0 ? null : count,
                        itemStyle: {
                            color: '#ef4444',
                            cursor: count > 0 ? 'pointer' : 'default'
                        }
                    })),
                    barWidth: '40%',
                    barMinHeight: 8
                },
                {
                    name: '进行中',
                    type: 'bar',
                    data: ongoingCounts.map((count, index) => ({
                        value: count === 0 ? null : count,
                        itemStyle: {
                            color: '#f59e0b',
                            cursor: count > 0 ? 'pointer' : 'default'
                        }
                    })),
                    barWidth: '40%',
                    barMinHeight: 8
                }
            ]
        }
    }

    // 排序币种
    const sortCoins = (coins) => {
        if (!coins) return []
        let filtered = filterOngoing ? coins.filter(c => c.ongoing) : coins
        return [...filtered].sort((a, b) => {
            if (sortBy === 'startTime') {
                // 按波段开始时间排序
                return sortOrder === 'desc'
                    ? b.waveStartTime - a.waveStartTime
                    : a.waveStartTime - b.waveStartTime
            } else {
                // 按涨幅排序
                return sortOrder === 'desc'
                    ? b.uptrendPercent - a.uptrendPercent
                    : a.uptrendPercent - b.uptrendPercent
            }
        })
    }

    // 获取排行数据
    const getRankingData = () => {
        // 显示特定币种的所有波段
        if (selectedSymbol && uptrendData?.allCoinsRanking) {
            const symbolWaves = uptrendData.allCoinsRanking.filter(c => c.symbol === selectedSymbol)
            return {
                title: `${selectedSymbol} 波段详情`,
                subtitle: `共 ${symbolWaves.length} 个波段`,
                coins: sortCoins(symbolWaves)
            }
        }
        if (showAllRanking && uptrendData?.allCoinsRanking) {
            return {
                title: '全部单边涨幅波段排行',
                subtitle: `共 ${uptrendData.totalCoins} 个波段`,
                coins: sortCoins(uptrendData.allCoinsRanking)
            }
        }
        if (selectedBucket) {
            return {
                title: selectedBucket.range,
                subtitle: `${selectedBucket.count} 个波段`,
                coins: sortCoins(selectedBucket.coins)
            }
        }
        return null
    }

    const rankingData = getRankingData()
    const isPanelOpen = showAllRanking || selectedBucket || selectedSymbol

    return (
        <div className="distribution-module uptrend-module">
            {/* 标题和控制栏 */}
            <div className="distribution-header">
                <div className="distribution-title">🚀 单边上行涨幅分布 <span style={{ fontSize: '12px', color: '#94a3b8' }}>（马丁做空参考）</span></div>
                <div className="time-base-selector">
                    <label style={{ display: 'flex', alignItems: 'center', marginRight: '8px' }}>
                        <input
                            type="checkbox"
                            checked={useCustomTime}
                            onChange={(e) => setUseCustomTime(e.target.checked)}
                            style={{ marginRight: '4px' }}
                        />
                        <span className="label">自定义时间</span>
                    </label>

                    {useCustomTime ? (
                        <>
                            <input
                                type="datetime-local"
                                className="time-input"
                                value={startTime}
                                onChange={(e) => setStartTime(e.target.value)}
                                style={{ width: '180px', marginRight: '4px' }}
                            />
                            <span style={{ color: '#94a3b8' }}>至</span>
                            <input
                                type="datetime-local"
                                className="time-input"
                                value={endTime}
                                onChange={(e) => setEndTime(e.target.value)}
                                style={{ width: '180px', marginLeft: '4px' }}
                            />
                        </>
                    ) : (
                        <>
                            <span className="label">时间范围:</span>
                            <select
                                className="time-select"
                                value={timeBase}
                                onChange={(e) => setTimeBase(Number(e.target.value))}
                            >
                                {TIME_OPTIONS.map(opt => (
                                    <option key={opt.value} value={opt.value}>
                                        {opt.label}
                                    </option>
                                ))}
                            </select>
                        </>
                    )}

                    <span className="label" style={{ marginLeft: '12px' }}>保留:</span>
                    <input
                        type="text"
                        className="threshold-input"
                        value={inputKeepRatio}
                        onChange={handleKeepRatioChange}
                        onBlur={applyKeepRatio}
                        onKeyDown={handleKeepRatioKeyDown}
                        style={{ width: '50px', textAlign: 'center' }}
                        title="位置比率低于此值视为波段结束"
                    />
                    <span style={{ color: '#94a3b8', marginLeft: '2px' }}>%</span>

                    <span className="label" style={{ marginLeft: '8px' }}>横盘:</span>
                    <input
                        type="text"
                        className="threshold-input"
                        value={inputNoNewHighCandles}
                        onChange={handleNoNewHighCandlesChange}
                        onBlur={applyNoNewHighCandles}
                        onKeyDown={handleNoNewHighCandlesKeyDown}
                        style={{ width: '45px', textAlign: 'center' }}
                        title="连续N根K线未创新高视为横盘结束"
                    />
                    <span style={{ color: '#94a3b8', marginLeft: '2px' }}>根</span>

                    <span className="label" style={{ marginLeft: '8px' }}>最小:</span>
                    <input
                        type="text"
                        className="threshold-input"
                        value={inputMinUptrend}
                        onChange={handleMinUptrendChange}
                        onBlur={applyMinUptrend}
                        onKeyDown={handleMinUptrendKeyDown}
                        style={{ width: '45px', textAlign: 'center' }}
                        title="最小涨幅过滤"
                    />
                    <span style={{ color: '#94a3b8', marginLeft: '2px' }}>%</span>

                    <button
                        className="refresh-btn"
                        onClick={fetchData}
                        disabled={loading}
                        title="刷新数据"
                        style={{ marginLeft: '12px' }}
                    >
                        {loading ? '⏳' : '🔄'}
                    </button>
                </div>
            </div>

            {/* 统计卡片 */}
            {uptrendData && (
                <div className="distribution-stats">
                    <div className="stat-item" style={{ borderLeft: '3px solid #ef4444' }}>
                        <span className="icon">📊</span>
                        <span className="label">总波段</span>
                        <span className="value">{uptrendData.totalCoins}</span>
                    </div>
                    <div className="stat-item" style={{ borderLeft: '3px solid #f59e0b' }}>
                        <span className="icon">🔴</span>
                        <span className="label">进行中</span>
                        <span className="value">{uptrendData.ongoingCount}</span>
                        <span className="percent">({((uptrendData.ongoingCount / uptrendData.totalCoins) * 100).toFixed(1)}%)</span>
                    </div>
                    <div className="stat-item" style={{ borderLeft: '3px solid #10b981' }}>
                        <span className="icon">📊</span>
                        <span className="label">平均涨幅</span>
                        <span className="value" style={{ color: '#10b981' }}>+{uptrendData.avgUptrend}%</span>
                    </div>
                    <div className="stat-item" style={{ borderLeft: '3px solid #6366f1' }}>
                        <span className="icon">🏆</span>
                        <span className="label">最大涨幅</span>
                        <span className="value" style={{ color: '#ef4444' }}>+{uptrendData.maxUptrend}%</span>
                    </div>
                </div>
            )}

            {/* 直方图 + 排行榜 */}
            <div className="distribution-charts">
                <div className={`chart-section ${isPanelOpen ? 'with-panel' : ''}`}>
                    <div className="section-title">
                        单边涨幅分布
                        <span style={{ fontSize: '12px', color: '#64748b', marginLeft: '8px' }}>(保留{Math.round(keepRatio * 100)}%涨幅 或 横盘{noNewHighCandles}根K线 视为波段结束)</span>

                        {/* 搜索框 */}
                        <div className="symbol-search">
                            <input
                                type="text"
                                className="search-input"
                                placeholder="搜索币种..."
                                value={searchSymbol}
                                onChange={(e) => setSearchSymbol(e.target.value)}
                                onKeyDown={handleSearchKeyDown}
                                style={{ width: '120px' }}
                            />
                            <button
                                className="search-btn"
                                onClick={handleSearchSymbol}
                                title="搜索币种的所有单边波段"
                            >
                                🔍
                            </button>
                        </div>

                        {uptrendData && (
                            <button
                                className="all-ranking-btn"
                                onClick={handleShowAllRanking}
                            >
                                🏆 查看全部排行
                            </button>
                        )}
                    </div>
                    {uptrendData ? (
                        <ReactECharts
                            ref={chartRef}
                            option={getHistogramOption()}
                            style={{ height: '300px', width: '100%' }}
                            opts={{ renderer: 'canvas' }}
                            onEvents={{ click: onChartClick }}
                        />
                    ) : (
                        <div className="chart-loading">加载中...</div>
                    )}
                </div>

                {/* 遮罩层 */}
                {isPanelOpen && (
                    <div className="ranking-overlay" onClick={closePanel} />
                )}

                {/* 排行榜面板 */}
                <div className={`ranking-panel ${isPanelOpen ? 'open' : ''}`}>
                    {rankingData && (
                        <>
                            <div className="ranking-header">
                                <div className="ranking-title">
                                    {selectedSymbol && (
                                        <button
                                            className="back-btn"
                                            onClick={() => {
                                                setSelectedSymbol(null)
                                                setShowAllRanking(true)
                                            }}
                                            title="返回全部排行"
                                        >
                                            ←
                                        </button>
                                    )}
                                    <span className={`range-badge ${showAllRanking ? 'all' : ''}`} style={{ background: 'linear-gradient(135deg, #ef4444, #f59e0b)' }}>{rankingData.title}</span>
                                    <span className="coin-count">{rankingData.subtitle}</span>
                                </div>
                                <button className="close-btn" onClick={closePanel}>✕</button>
                            </div>
                            {/* 筛选和排序 */}
                            <div className="sort-controls">
                                <label className="filter-ongoing">
                                    <input
                                        type="checkbox"
                                        checked={filterOngoing}
                                        onChange={(e) => setFilterOngoing(e.target.checked)}
                                    />
                                    <span>只看进行中</span>
                                </label>
                                <div className="sort-type-toggle">
                                    <button
                                        className={`sort-type-btn ${sortBy === 'uptrend' ? 'active' : ''}`}
                                        onClick={() => setSortBy('uptrend')}
                                        title="按涨幅排序"
                                    >
                                        📈涨幅
                                    </button>
                                    <button
                                        className={`sort-type-btn ${sortBy === 'startTime' ? 'active' : ''}`}
                                        onClick={() => setSortBy('startTime')}
                                        title="按波段开始时间排序"
                                    >
                                        🕐时间
                                    </button>
                                </div>
                                <button
                                    className="sort-order-btn"
                                    onClick={() => setSortOrder(sortOrder === 'desc' ? 'asc' : 'desc')}
                                    title={sortOrder === 'desc' ? '当前：降序，点击切换升序' : '当前：升序，点击切换降序'}
                                >
                                    {sortOrder === 'desc' ? '↓' : '↑'}
                                </button>
                            </div>
                            <div className="ranking-list">
                                {rankingData.coins.map((coin, index) => (
                                    <div
                                        key={`${coin.symbol}-${coin.waveStartTime}-${index}`}
                                        className="ranking-item uptrend-item"
                                    >
                                        <span className="rank">{index + 1}</span>
                                        <div className="coin-info" onClick={() => handleCopySymbol(coin.symbol)} title="点击复制币名">
                                            <span className="symbol">
                                                {coin.symbol}
                                                {coin.ongoing && <span className="ongoing-badge">🔴进行中</span>}
                                            </span>
                                            <span className="time-range">
                                                {formatTimestamp(coin.waveStartTime)} → {formatTimestamp(coin.waveEndTime)}
                                            </span>
                                        </div>
                                        <div className="uptrend-value">
                                            <span className="percent">+{coin.uptrendPercent.toFixed(2)}%</span>
                                            <span className="price-range">
                                                {coin.startPrice?.toFixed(4)} → {coin.peakPrice?.toFixed(4)}
                                            </span>
                                        </div>
                                        {!selectedSymbol && (
                                            <button
                                                className="detail-btn"
                                                onClick={(e) => {
                                                    e.stopPropagation()
                                                    handleViewSymbolWaves(coin.symbol)
                                                }}
                                                title="查看该币种所有波段"
                                            >
                                                📊
                                            </button>
                                        )}
                                        {copiedSymbol === coin.symbol && (
                                            <span className="copied-tip">已复制!</span>
                                        )}
                                    </div>
                                ))}
                                {rankingData.coins.length === 0 && (
                                    <div className="no-data">暂无数据</div>
                                )}
                            </div>
                        </>
                    )}
                </div>
            </div>
        </div>
    )
}

export default UptrendModule
