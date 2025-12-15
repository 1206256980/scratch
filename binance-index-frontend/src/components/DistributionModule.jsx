import { useState, useEffect, useCallback, useRef } from 'react'
import axios from 'axios'
import ReactECharts from 'echarts-for-react'

// 时间选项配置
const TIME_OPTIONS = [
    { label: '30分钟', value: 0.5 },
    { label: '1小时', value: 1 },
    { label: '2小时', value: 2 },
    { label: '4小时', value: 4 },
    { label: '6小时', value: 6 },
    { label: '12小时', value: 12 },
    { label: '1天', value: 24 },
    { label: '3天', value: 72 },
    { label: '7天', value: 168 }
]

function DistributionModule() {
    const [timeBase, setTimeBase] = useState(24) // 默认24小时
    const [distributionData, setDistributionData] = useState(null)
    const [loading, setLoading] = useState(false)
    const [selectedBucket, setSelectedBucket] = useState(null) // 选中的区间
    const [showAllRanking, setShowAllRanking] = useState(false) // 显示全部排行榜
    const [copiedSymbol, setCopiedSymbol] = useState(null) // 复制提示
    const [sortType, setSortType] = useState('current') // 排序类型: current, max, min
    const [sortOrder, setSortOrder] = useState('desc') // 排序方向: asc, desc
    const chartRef = useRef(null)

    // 获取分布数据
    const fetchDistribution = useCallback(async () => {
        setLoading(true)
        setSelectedBucket(null) // 切换时间时关闭面板
        setShowAllRanking(false)
        try {
            const res = await axios.get(`/api/index/distribution?hours=${timeBase}`)
            if (res.data.success) {
                setDistributionData(res.data.data)
            }
        } catch (err) {
            console.error('获取分布数据失败:', err)
        }
        setLoading(false)
    }, [timeBase])

    useEffect(() => {
        fetchDistribution()
    }, [fetchDistribution])

    // 复制币种名称
    const handleCopySymbol = async (symbol) => {
        try {
            await navigator.clipboard.writeText(symbol)
            setCopiedSymbol(symbol)
            setTimeout(() => setCopiedSymbol(null), 1500)
        } catch (err) {
            console.error('复制失败:', err)
        }
    }

    // 关闭排行榜面板
    const closePanel = () => {
        setSelectedBucket(null)
        setShowAllRanking(false)
    }

    // 图表点击事件 - 支持点击柱子和 X 轴区域
    const onChartClick = (params) => {
        if (!distributionData || !distributionData.distribution) return

        // 获取点击的数据索引
        let dataIndex = params.dataIndex

        // 如果点击的是 X 轴标签
        if (params.componentType === 'xAxis') {
            dataIndex = params.dataIndex
        }

        if (dataIndex !== undefined && dataIndex !== null) {
            const bucket = distributionData.distribution[dataIndex]
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
        if (!distributionData || !distributionData.distribution) {
            return {}
        }

        const distribution = distributionData.distribution
        const ranges = distribution.map(d => d.range)
        const counts = distribution.map(d => d.count)

        // 根据区间设置颜色
        const colors = distribution.map(d => {
            const range = d.range
            if (range.includes('<') || (range.includes('-') && !range.startsWith('-5') && !range.startsWith('0'))) {
                // 负值区间用红色
                if (range.includes('-50') || range.includes('-45') || range.includes('-40') ||
                    range.includes('-35') || range.includes('-30') || range.includes('-25') ||
                    range.includes('-20') || range.includes('-15') || range.includes('-10') ||
                    range.includes('-5%~0%')) {
                    return '#ef4444'
                }
            }
            if (range.includes('>') || range.startsWith('0%~') || range.startsWith('5') ||
                range.startsWith('10') || range.startsWith('15') || range.startsWith('20') ||
                range.startsWith('25') || range.startsWith('30') || range.startsWith('35') ||
                range.startsWith('40') || range.startsWith('45')) {
                return '#10b981'
            }
            return '#64748b'
        })

        return {
            backgroundColor: 'transparent',
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'shadow'  // 阴影指示器，更容易看到悬浮区域
                },
                backgroundColor: 'rgba(22, 27, 34, 0.95)',
                borderColor: 'rgba(99, 102, 241, 0.3)',
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
                        <div style="font-weight: 600; margin-bottom: 8px;">${bucket.range}</div>
                        <div>币种数量: <span style="color: #6366f1; font-weight: 600;">${bucket.count}</span></div>`
                    if (bucket.coins && bucket.coins.length > 0) {
                        const displayCoins = bucket.coins.slice(0, 20)
                        const moreCount = bucket.coins.length - 20
                        // 每4个一行显示
                        let coinsHtml = '<div style="margin-top: 6px; font-size: 11px; color: #94a3b8;">'
                        for (let i = 0; i < displayCoins.length; i += 4) {
                            const row = displayCoins.slice(i, i + 4).join(', ')
                            coinsHtml += `<div style="margin: 2px 0;">${row}</div>`
                        }
                        if (moreCount > 0) {
                            coinsHtml += `<div style="margin-top: 4px; color: #64748b;">等 ${moreCount} 个...</div>`
                        }
                        coinsHtml += '</div>'
                        html += coinsHtml
                    }
                    html += '<div style="font-size: 11px; color: #10b981; margin-top: 6px; font-weight: 500;">👆 点击查看完整排行</div>'
                    html += '</div>'
                    return html
                }
            },
            grid: {
                left: '3%',
                right: '4%',
                top: '10%',
                bottom: '15%',
                containLabel: true
            },
            xAxis: {
                type: 'category',
                data: ranges,
                axisLabel: {
                    color: '#64748b',
                    rotate: 45,
                    fontSize: 10
                },
                axisLine: { lineStyle: { color: 'rgba(100, 116, 139, 0.2)' } },
                triggerEvent: true  // 开启 X 轴事件，支持点击 X 轴标签
            },
            yAxis: {
                type: 'value',
                name: '币种数',
                nameTextStyle: { color: '#64748b' },
                axisLabel: { color: '#64748b' },
                splitLine: { lineStyle: { color: 'rgba(100, 116, 139, 0.1)' } }
            },
            series: [{
                type: 'bar',
                data: counts.map((count, index) => ({
                    // 0 值设为 null，不显示柱子
                    value: count === 0 ? null : count,
                    itemStyle: {
                        color: colors[index],
                        cursor: count > 0 ? 'pointer' : 'default'
                    },
                    // 当柱子有值但太小时，显示一个标记
                    label: count > 0 && count <= Math.max(...counts) * 0.05 ? {
                        show: true,
                        position: 'top',
                        formatter: count.toString(),
                        color: colors[index],
                        fontSize: 12,
                        fontWeight: 'bold'
                    } : { show: false }
                })),
                barWidth: '60%',
                barMinHeight: 8  // 只对有值的柱子生效
            }]
        }
    }

    // 根据排序类型获取排序值
    const getSortValue = (coin) => {
        switch (sortType) {
            case 'max': return coin.maxChangePercent || 0
            case 'min': return coin.minChangePercent || 0
            default: return coin.changePercent || 0
        }
    }

    // 排序币种列表
    const sortCoins = (coins) => {
        if (!coins) return []
        return [...coins].sort((a, b) => {
            const valA = getSortValue(a)
            const valB = getSortValue(b)
            return sortOrder === 'desc' ? valB - valA : valA - valB
        })
    }

    // 获取当前要显示的排行数据
    const getRankingData = () => {
        if (showAllRanking && distributionData?.allCoinsRanking) {
            return {
                title: '全部币种涨跌幅排行',
                subtitle: `共 ${distributionData.totalCoins} 个币种`,
                coins: sortCoins(distributionData.allCoinsRanking)
            }
        }
        if (selectedBucket) {
            return {
                title: selectedBucket.range,
                subtitle: `${selectedBucket.count} 个币种`,
                coins: sortCoins(selectedBucket.coinDetails)
            }
        }
        return null
    }

    const rankingData = getRankingData()
    const isPanelOpen = showAllRanking || selectedBucket

    return (
        <div className="distribution-module">
            {/* 时间选择器 */}
            <div className="distribution-header">
                <div className="distribution-title">📊 涨幅分布分析</div>
                <div className="time-base-selector">
                    <span className="label">基准时间:</span>
                    {TIME_OPTIONS.map(opt => (
                        <button
                            key={opt.value}
                            className={`time-btn ${timeBase === opt.value ? 'active' : ''}`}
                            onClick={() => setTimeBase(opt.value)}
                        >
                            {opt.label}
                        </button>
                    ))}
                    {loading && <span className="loading-text">加载中...</span>}
                </div>
            </div>

            {/* 涨跌统计 */}
            {distributionData && (
                <div className="distribution-stats">
                    <div className="stat-item up">
                        <span className="icon">📈</span>
                        <span className="label">上涨币种</span>
                        <span className="value">{distributionData.upCount}</span>
                        <span className="percent">({((distributionData.upCount / distributionData.totalCoins) * 100).toFixed(1)}%)</span>
                    </div>
                    <div className="stat-item down">
                        <span className="icon">📉</span>
                        <span className="label">下跌币种</span>
                        <span className="value">{distributionData.downCount}</span>
                        <span className="percent">({((distributionData.downCount / distributionData.totalCoins) * 100).toFixed(1)}%)</span>
                    </div>
                    <div className="stat-item total">
                        <span className="icon">🪙</span>
                        <span className="label">总币种</span>
                        <span className="value">{distributionData.totalCoins}</span>
                    </div>
                </div>
            )}

            {/* 直方图 + 排行榜面板 */}
            <div className="distribution-charts">
                <div className={`chart-section ${isPanelOpen ? 'with-panel' : ''}`}>
                    <div className="section-title">
                        涨幅分布直方图
                        <span style={{ fontSize: '12px', color: '#64748b', marginLeft: '8px' }}>(点击柱子查看详情)</span>
                        {distributionData && (
                            <button
                                className="all-ranking-btn"
                                onClick={handleShowAllRanking}
                            >
                                🏆 查看全部排行
                            </button>
                        )}
                    </div>
                    {distributionData ? (
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

                {/* 遮罩层 - 点击关闭侧边栏 */}
                {isPanelOpen && (
                    <div className="ranking-overlay" onClick={closePanel} />
                )}

                {/* 排行榜滑出面板 */}
                <div className={`ranking-panel ${isPanelOpen ? 'open' : ''}`}>
                    {rankingData && (
                        <>
                            <div className="ranking-header">
                                <div className="ranking-title">
                                    <span className={`range-badge ${showAllRanking ? 'all' : ''}`}>{rankingData.title}</span>
                                    <span className="coin-count">{rankingData.subtitle}</span>
                                </div>
                                <button className="close-btn" onClick={closePanel}>✕</button>
                            </div>
                            {/* 排序切换栏 */}
                            <div className="sort-controls">
                                <div className="sort-type-group">
                                    <button
                                        className={`sort-btn ${sortType === 'current' ? 'active' : ''}`}
                                        onClick={() => setSortType('current')}
                                    >
                                        当前
                                    </button>
                                    <button
                                        className={`sort-btn ${sortType === 'max' ? 'active' : ''}`}
                                        onClick={() => setSortType('max')}
                                    >
                                        最高
                                    </button>
                                    <button
                                        className={`sort-btn ${sortType === 'min' ? 'active' : ''}`}
                                        onClick={() => setSortType('min')}
                                    >
                                        最低
                                    </button>
                                </div>
                                <button
                                    className="sort-order-btn"
                                    onClick={() => setSortOrder(sortOrder === 'desc' ? 'asc' : 'desc')}
                                    title={sortOrder === 'desc' ? '降序' : '升序'}
                                >
                                    {sortOrder === 'desc' ? '↓' : '↑'}
                                </button>
                            </div>
                            <div className="ranking-list">
                                {rankingData.coins.map((coin, index) => (
                                    <div
                                        key={coin.symbol}
                                        className="ranking-item"
                                        onClick={() => handleCopySymbol(coin.symbol)}
                                        title="点击复制"
                                    >
                                        <span className="rank">{index + 1}</span>
                                        <span className="symbol">{coin.symbol}</span>
                                        <div className="change-group">
                                            <span className={`change current ${coin.changePercent >= 0 ? 'positive' : 'negative'}`}>
                                                {coin.changePercent >= 0 ? '+' : ''}{coin.changePercent.toFixed(2)}%
                                            </span>
                                            <span className={`change max ${coin.maxChangePercent >= 0 ? 'positive' : 'negative'}`} title="最高涨幅">
                                                ↑{coin.maxChangePercent >= 0 ? '+' : ''}{coin.maxChangePercent.toFixed(2)}%
                                            </span>
                                            <span className={`change min ${coin.minChangePercent >= 0 ? 'positive' : 'negative'}`} title="最低涨幅">
                                                ↓{coin.minChangePercent >= 0 ? '+' : ''}{coin.minChangePercent.toFixed(2)}%
                                            </span>
                                        </div>
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

export default DistributionModule
