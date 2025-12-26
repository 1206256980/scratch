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

// 时间粒度选项（波段启动时间分布图表）
const TIME_GRANULARITY_OPTIONS = [
    { label: '自动', value: 'auto' },
    { label: '2小时', value: 2 },
    { label: '4小时', value: 4 },
    { label: '8小时', value: 8 },
    { label: '12小时', value: 12 },
    { label: '1天', value: 24 }
]

// 格式化时间戳为本地时间
const formatTimestamp = (ts) => {
    if (!ts) return '--'
    const date = new Date(ts)
    const pad = (n) => String(n).padStart(2, '0')
    return `${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}`
}

// 格式化用时（毫秒 -> 可读格式）
const formatDuration = (startTs, endTs) => {
    if (!startTs || !endTs) return '--'
    const ms = endTs - startTs
    if (ms <= 0) return '--'

    const minutes = Math.floor(ms / (1000 * 60))
    const hours = Math.floor(minutes / 60)
    const days = Math.floor(hours / 24)

    if (days >= 1) {
        const remainHours = hours % 24
        return `${days}天${remainHours}时`
    } else if (hours >= 1) {
        const remainMinutes = minutes % 60
        return `${hours}时${remainMinutes}分`
    } else {
        return `${minutes}分钟`
    }
}

function UptrendModule() {
    // 从 localStorage 读取缓存值的工具函数
    const getCache = (key, defaultValue) => {
        try {
            const cached = localStorage.getItem(`uptrend_${key}`)
            if (cached !== null) {
                return JSON.parse(cached)
            }
        } catch (e) {
            console.warn(`读取缓存失败: ${key}`, e)
        }
        return defaultValue
    }

    const [timeBase, setTimeBase] = useState(() => getCache('timeBase', 24))
    const [useCustomTime, setUseCustomTime] = useState(() => getCache('useCustomTime', false))
    const [startTime, setStartTime] = useState(() => getCache('startTime', ''))
    const [endTime, setEndTime] = useState(() => getCache('endTime', ''))
    const [keepRatio, setKeepRatio] = useState(() => getCache('keepRatio', 0.75))
    const [inputKeepRatio, setInputKeepRatio] = useState(() => String(Math.round(getCache('keepRatio', 0.75) * 100)))
    const [noNewHighCandles, setNoNewHighCandles] = useState(() => getCache('noNewHighCandles', 6))
    const [inputNoNewHighCandles, setInputNoNewHighCandles] = useState(() => String(getCache('noNewHighCandles', 6)))
    const [minUptrend, setMinUptrend] = useState(() => getCache('minUptrend', 4))
    const [inputMinUptrend, setInputMinUptrend] = useState(() => String(getCache('minUptrend', 4)))
    const [uptrendData, setUptrendData] = useState(null)
    const [loading, setLoading] = useState(false)
    const [selectedBucket, setSelectedBucket] = useState(null)
    const [showAllRanking, setShowAllRanking] = useState(false)
    const [copiedSymbol, setCopiedSymbol] = useState(null)
    const [sortOrder, setSortOrder] = useState('desc')
    const [sortBy, setSortBy] = useState('uptrend')
    const [filterOngoing, setFilterOngoing] = useState(false)
    const [selectedSymbol, setSelectedSymbol] = useState(null)
    const [searchSymbol, setSearchSymbol] = useState('')
    const [timeChartThreshold, setTimeChartThreshold] = useState(() => getCache('timeChartThreshold', 10))
    const [inputTimeChartThreshold, setInputTimeChartThreshold] = useState(() => String(getCache('timeChartThreshold', 10)))
    const [timeGranularity, setTimeGranularity] = useState(() => getCache('timeGranularity', 'auto'))
    const [selectedTimeBucket, setSelectedTimeBucket] = useState(null)
    const [winRate, setWinRate] = useState(() => getCache('winRate', 90))
    const [inputWinRate, setInputWinRate] = useState(() => String(getCache('winRate', 90)))
    const chartRef = useRef(null)
    const timeChartRef = useRef(null)
    const listRef = useRef(null)
    const [displayCount, setDisplayCount] = useState(50) // 初始显示50条
    const PAGE_SIZE = 50 // 每次加载50条

    // 保存设置到 localStorage
    useEffect(() => {
        localStorage.setItem('uptrend_timeBase', JSON.stringify(timeBase))
    }, [timeBase])

    useEffect(() => {
        localStorage.setItem('uptrend_useCustomTime', JSON.stringify(useCustomTime))
    }, [useCustomTime])

    useEffect(() => {
        if (startTime) localStorage.setItem('uptrend_startTime', JSON.stringify(startTime))
    }, [startTime])

    useEffect(() => {
        if (endTime) localStorage.setItem('uptrend_endTime', JSON.stringify(endTime))
    }, [endTime])

    useEffect(() => {
        localStorage.setItem('uptrend_keepRatio', JSON.stringify(keepRatio))
    }, [keepRatio])

    useEffect(() => {
        localStorage.setItem('uptrend_noNewHighCandles', JSON.stringify(noNewHighCandles))
    }, [noNewHighCandles])

    useEffect(() => {
        localStorage.setItem('uptrend_minUptrend', JSON.stringify(minUptrend))
    }, [minUptrend])

    useEffect(() => {
        localStorage.setItem('uptrend_timeChartThreshold', JSON.stringify(timeChartThreshold))
    }, [timeChartThreshold])

    useEffect(() => {
        localStorage.setItem('uptrend_timeGranularity', JSON.stringify(timeGranularity))
    }, [timeGranularity])

    useEffect(() => {
        localStorage.setItem('uptrend_winRate', JSON.stringify(winRate))
    }, [winRate])

    // 重置所有设置为默认值并清除缓存
    const resetToDefaults = () => {
        // 清除所有缓存
        const keys = ['timeBase', 'useCustomTime', 'startTime', 'endTime', 'keepRatio',
            'noNewHighCandles', 'minUptrend', 'timeChartThreshold', 'timeGranularity', 'winRate']
        keys.forEach(key => localStorage.removeItem(`uptrend_${key}`))

        // 恢复默认值
        setTimeBase(24)
        setUseCustomTime(false)
        setStartTime('')
        setEndTime('')
        setKeepRatio(0.75)
        setInputKeepRatio('75')
        setNoNewHighCandles(6)
        setInputNoNewHighCandles('6')
        setMinUptrend(4)
        setInputMinUptrend('4')
        setTimeChartThreshold(10)
        setInputTimeChartThreshold('10')
        setTimeGranularity('auto')
        setWinRate(90)
        setInputWinRate('90')
    }


    // 获取数据 (silent: 静默刷新，不显示loading，不重置选择状态)
    const fetchData = useCallback(async (silent = false) => {
        if (!silent) {
            setLoading(true)
            setSelectedBucket(null)
            setShowAllRanking(false)
            setSelectedSymbol(null)
        }
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
        if (!silent) {
            setLoading(false)
        }
    }, [timeBase, keepRatio, noNewHighCandles, minUptrend, useCustomTime, startTime, endTime])

    useEffect(() => {
        fetchData()

        // 每1分钟静默刷新
        const interval = setInterval(() => {
            fetchData(true)  // 静默刷新
        }, 60000)

        return () => clearInterval(interval)
    }, [fetchData])

    // 侧边栏打开时锁定背景滚动
    useEffect(() => {
        const isPanelOpen = showAllRanking || selectedBucket || selectedSymbol || selectedTimeBucket
        if (isPanelOpen) {
            document.body.style.overflow = 'hidden'
        } else {
            document.body.style.overflow = ''
        }
        return () => {
            document.body.style.overflow = ''
        }
    }, [showAllRanking, selectedBucket, selectedSymbol, selectedTimeBucket])

    // 切换筛选条件时重置显示条数
    useEffect(() => {
        setDisplayCount(PAGE_SIZE)
    }, [showAllRanking, selectedBucket, selectedSymbol, selectedTimeBucket, sortBy, sortOrder, filterOngoing])

    // 滚动加载更多
    const handleListScroll = useCallback((e) => {
        const { scrollTop, scrollHeight, clientHeight } = e.target
        // 滚动到底部100px内时加载更多
        if (scrollHeight - scrollTop - clientHeight < 100) {
            setDisplayCount(prev => prev + PAGE_SIZE)
        }
    }, [])

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
        setSelectedTimeBucket(null)
    }

    // 处理时间图表涨幅阈值输入
    const handleTimeChartThresholdChange = (e) => {
        setInputTimeChartThreshold(e.target.value)
    }

    const applyTimeChartThreshold = () => {
        const val = parseFloat(inputTimeChartThreshold)
        if (!isNaN(val) && val >= 0 && val <= 100) {
            setTimeChartThreshold(val)
        } else {
            setInputTimeChartThreshold(String(timeChartThreshold))
        }
    }

    const handleTimeChartThresholdKeyDown = (e) => {
        if (e.key === 'Enter') applyTimeChartThreshold()
    }

    // 处理胜率输入
    const handleWinRateChange = (e) => {
        setInputWinRate(e.target.value)
    }

    const applyWinRate = () => {
        const val = parseFloat(inputWinRate)
        if (!isNaN(val) && val >= 50 && val <= 99) {
            setWinRate(val)
        } else {
            setInputWinRate(String(winRate))
        }
    }

    const handleWinRateKeyDown = (e) => {
        if (e.key === 'Enter') applyWinRate()
    }

    // 计算分位数涨幅（胜率分析）
    // 例如90%胜率：按涨幅从大到小排序，取第(1-90%)=10%位置的涨幅
    // 意思是历史上有90%的波段涨幅不超过这个值
    const calculateWinRateUptrend = () => {
        if (!uptrendData?.allCoinsRanking || uptrendData.allCoinsRanking.length === 0) {
            return null
        }

        const allWaves = uptrendData.allCoinsRanking
        // 按涨幅从大到小排序
        const sorted = [...allWaves].sort((a, b) => b.uptrendPercent - a.uptrendPercent)

        // 计算分位数位置
        // 90%胜率 = 取第10%的位置
        const position = Math.ceil(sorted.length * (1 - winRate / 100))
        const index = Math.max(0, Math.min(position - 1, sorted.length - 1))

        return {
            uptrend: sorted[index].uptrendPercent.toFixed(1),
            position: position,
            total: sorted.length
        }
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
            } else if (sortBy === 'duration') {
                // 按波段持续时间排序
                const aDuration = (a.waveEndTime || 0) - (a.waveStartTime || 0)
                const bDuration = (b.waveEndTime || 0) - (b.waveStartTime || 0)
                return sortOrder === 'desc'
                    ? bDuration - aDuration
                    : aDuration - bDuration
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
        // 时间桶选中
        if (selectedTimeBucket) {
            return {
                title: `${selectedTimeBucket.label} 启动的波段`,
                subtitle: `${selectedTimeBucket.count} 个波段`,
                coins: sortCoins(selectedTimeBucket.coins)
            }
        }
        return null
    }

    // 获取选中币种的统计数据
    const getSymbolStats = () => {
        if (!selectedSymbol || !uptrendData?.allCoinsRanking) return null

        const symbolWaves = uptrendData.allCoinsRanking.filter(c => c.symbol === selectedSymbol)
        if (symbolWaves.length === 0) return null

        // 波段总数
        const totalWaves = symbolWaves.length

        // 进行中波段数
        const ongoingCount = symbolWaves.filter(w => w.ongoing).length

        // 计算平均涨幅
        const avgUptrend = symbolWaves.reduce((sum, w) => sum + w.uptrendPercent, 0) / totalWaves

        // 最大涨幅
        const maxUptrend = Math.max(...symbolWaves.map(w => w.uptrendPercent))

        // 计算用时统计（只计算已结束的波段）
        const completedWaves = symbolWaves.filter(w => w.waveEndTime && w.waveStartTime && !w.ongoing)
        const durations = completedWaves.map(w => w.waveEndTime - w.waveStartTime)

        const avgDurationMs = durations.length > 0
            ? durations.reduce((a, b) => a + b, 0) / durations.length
            : 0

        // 格式化用时
        const formatDurationLocal = (ms) => {
            if (!ms || ms <= 0) return '--'
            const hours = Math.floor(ms / (1000 * 60 * 60))
            const minutes = Math.floor((ms % (1000 * 60 * 60)) / (1000 * 60))
            if (hours >= 24) {
                const days = Math.floor(hours / 24)
                const remainHours = hours % 24
                return `${days}天${remainHours}时`
            }
            return hours > 0 ? `${hours}时${minutes}分` : `${minutes}分钟`
        }

        // 最近启动时间
        const latestStartTime = Math.max(...symbolWaves.map(w => w.waveStartTime))
        // 首次启动时间
        const firstStartTime = Math.min(...symbolWaves.map(w => w.waveStartTime))

        return {
            totalWaves,
            ongoingCount,
            avgUptrend: avgUptrend.toFixed(1),
            maxUptrend: maxUptrend.toFixed(1),
            avgDuration: formatDurationLocal(avgDurationMs),
            latestStartTime,
            firstStartTime
        }
    }


    // 计算时间分布数据
    const getTimeDistributionData = () => {
        if (!uptrendData?.allCoinsRanking) return null

        // 过滤符合阈值的波段
        const filteredWaves = uptrendData.allCoinsRanking.filter(
            c => c.uptrendPercent >= timeChartThreshold
        )

        if (filteredWaves.length === 0) return null

        // 获取时间范围
        const minTime = Math.min(...filteredWaves.map(c => c.waveStartTime))
        const maxTime = Math.max(...filteredWaves.map(c => c.waveStartTime))
        const rangeHours = (maxTime - minTime) / (1000 * 60 * 60)

        // 根据用户选择或时间范围确定粒度
        let bucketSizeMs
        let bucketLabel

        if (timeGranularity !== 'auto') {
            // 用户手动选择了粒度
            const hours = Number(timeGranularity)
            bucketSizeMs = hours * 60 * 60 * 1000
            bucketLabel = hours >= 24 ? `${hours / 24}天` : `${hours}小时`
        } else {
            // 自动确定粒度
            if (rangeHours <= 6) {
                bucketSizeMs = 30 * 60 * 1000 // 30分钟
                bucketLabel = '30分钟'
            } else if (rangeHours <= 24) {
                bucketSizeMs = 60 * 60 * 1000 // 1小时
                bucketLabel = '1小时'
            } else if (rangeHours <= 72) {
                bucketSizeMs = 2 * 60 * 60 * 1000 // 2小时
                bucketLabel = '2小时'
            } else if (rangeHours <= 168) {
                bucketSizeMs = 4 * 60 * 60 * 1000 // 4小时
                bucketLabel = '4小时'
            } else {
                bucketSizeMs = 12 * 60 * 60 * 1000 // 12小时
                bucketLabel = '12小时'
            }
        }

        // 对齐起始时间
        let alignedMin, alignedMax
        if (bucketSizeMs >= 24 * 60 * 60 * 1000) {
            // 1天或更大粒度：按自然日对齐到00:00
            const minDate = new Date(minTime)
            minDate.setHours(0, 0, 0, 0)
            alignedMin = minDate.getTime()

            const maxDate = new Date(maxTime)
            maxDate.setHours(0, 0, 0, 0)
            maxDate.setDate(maxDate.getDate() + 1) // 下一天的00:00
            alignedMax = maxDate.getTime()
        } else {
            // 其他粒度：按时间戳对齐
            alignedMin = Math.floor(minTime / bucketSizeMs) * bucketSizeMs
            alignedMax = Math.ceil(maxTime / bucketSizeMs) * bucketSizeMs
        }

        // 创建时间桶
        const buckets = []
        for (let t = alignedMin; t < alignedMax; t += bucketSizeMs) {
            const bucketStart = t
            const bucketEnd = t + bucketSizeMs
            const wavesInBucket = filteredWaves.filter(
                c => c.waveStartTime >= bucketStart && c.waveStartTime < bucketEnd
            )

            const date = new Date(bucketStart)
            const pad = (n) => String(n).padStart(2, '0')
            let label
            if (bucketSizeMs >= 24 * 60 * 60 * 1000) {
                // 1天或更大粒度：只显示日期
                label = `${pad(date.getMonth() + 1)}-${pad(date.getDate())}`
            } else if (bucketSizeMs >= 12 * 60 * 60 * 1000) {
                label = `${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:00`
            } else {
                label = `${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}`
            }

            buckets.push({
                label,
                startTime: bucketStart,
                endTime: bucketEnd,
                count: wavesInBucket.length,
                ongoingCount: wavesInBucket.filter(c => c.ongoing).length,
                coins: wavesInBucket
            })
        }

        // 计算用时统计（从 waveStartTime 到 waveEndTime）
        const durations = filteredWaves
            .filter(c => c.waveEndTime && c.waveStartTime)
            .map(c => c.waveEndTime - c.waveStartTime)

        const avgDurationMs = durations.length > 0
            ? durations.reduce((a, b) => a + b, 0) / durations.length
            : 0
        const maxDurationMs = durations.length > 0
            ? Math.max(...durations)
            : 0

        // 格式化用时
        const formatDuration = (ms) => {
            const hours = Math.floor(ms / (1000 * 60 * 60))
            const minutes = Math.floor((ms % (1000 * 60 * 60)) / (1000 * 60))
            if (hours >= 24) {
                const days = Math.floor(hours / 24)
                const remainHours = hours % 24
                return `${days}天${remainHours}时`
            }
            return hours > 0 ? `${hours}时${minutes}分` : `${minutes}分钟`
        }

        // 计算平均涨幅
        const avgUptrend = filteredWaves.reduce((sum, c) => sum + c.uptrendPercent, 0) / filteredWaves.length

        // 找出最热时段
        let hottestBucket = buckets[0]
        buckets.forEach(b => {
            if (b.count > hottestBucket.count) {
                hottestBucket = b
            }
        })

        return {
            buckets,
            bucketLabel,
            totalWaves: filteredWaves.length,
            avgDuration: formatDuration(avgDurationMs),
            maxDuration: formatDuration(maxDurationMs),
            avgUptrend: avgUptrend.toFixed(1),
            hottestPeriod: hottestBucket?.label || '--',
            hottestCount: hottestBucket?.count || 0,
            ongoingTotal: filteredWaves.filter(c => c.ongoing).length
        }
    }

    // 时间分布图表配置
    const getTimeDistributionOption = () => {
        const data = getTimeDistributionData()
        if (!data) return {}

        const { buckets } = data
        const labels = buckets.map(b => b.label)
        const counts = buckets.map(b => b.count)
        const ongoingCounts = buckets.map(b => b.ongoingCount)

        return {
            backgroundColor: 'transparent',
            tooltip: {
                trigger: 'axis',
                axisPointer: { type: 'shadow' },
                backgroundColor: 'rgba(22, 27, 34, 0.95)',
                borderColor: 'rgba(16, 185, 129, 0.3)',
                textStyle: { color: '#f1f5f9' },
                formatter: function (params) {
                    if (!params || params.length === 0) return ''
                    const param = params[0]
                    const bucket = buckets[param.dataIndex]
                    if (!bucket || bucket.count === 0) {
                        return `<div style="padding: 8px;">
                            <div style="font-weight: 600;">${bucket.label}</div>
                            <div style="color: #94a3b8;">该时段暂无波段启动</div>
                        </div>`
                    }
                    let html = `<div style="padding: 8px; max-width: 320px;">
                        <div style="font-weight: 600; margin-bottom: 8px; color: #10b981;">🕐 ${bucket.label}</div>
                        <div>波段数: <span style="color: #10b981; font-weight: 600;">${bucket.count}</span></div>
                        <div>进行中: <span style="color: #f59e0b; font-weight: 600;">${bucket.ongoingCount || 0}</span></div>`
                    if (bucket.coins && bucket.coins.length > 0) {
                        const displayCoins = bucket.coins.slice(0, 8)
                        const moreCount = bucket.coins.length - 8
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
                    html += '<div style="font-size: 11px; color: #10b981; margin-top: 6px; font-weight: 500;">👆 点击查看完整列表</div>'
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
                bottom: '18%',
                containLabel: true
            },
            xAxis: {
                type: 'category',
                data: labels,
                axisLabel: {
                    color: '#64748b',
                    rotate: 30,
                    fontSize: 10
                },
                axisLine: { lineStyle: { color: 'rgba(100, 116, 139, 0.2)' } },
                triggerEvent: true
            },
            yAxis: {
                type: 'value',
                name: '波段数',
                nameTextStyle: { color: '#64748b' },
                axisLabel: { color: '#64748b' },
                splitLine: { lineStyle: { color: 'rgba(100, 116, 139, 0.1)' } }
            },
            series: [
                {
                    name: '总数',
                    type: 'bar',
                    data: counts.map((count) => ({
                        value: count === 0 ? null : count,
                        itemStyle: {
                            color: '#10b981',
                            cursor: count > 0 ? 'pointer' : 'default'
                        }
                    })),
                    barWidth: '50%',
                    barMinHeight: 8
                },
                {
                    name: '进行中',
                    type: 'bar',
                    data: ongoingCounts.map((count) => ({
                        value: count === 0 ? null : count,
                        itemStyle: {
                            color: '#f59e0b',
                            cursor: count > 0 ? 'pointer' : 'default'
                        }
                    })),
                    barWidth: '50%',
                    barMinHeight: 8
                }
            ]
        }
    }

    // 时间图表点击事件
    const onTimeChartClick = (params) => {
        const data = getTimeDistributionData()
        if (!data) return

        let dataIndex = params.dataIndex
        if (dataIndex !== undefined && dataIndex !== null) {
            const bucket = data.buckets[dataIndex]
            if (bucket && bucket.count > 0) {
                setSelectedSymbol(null)
                setSelectedBucket(null)
                setShowAllRanking(false)
                setSelectedTimeBucket(bucket)
            }
        }
    }

    const rankingData = getRankingData()
    const symbolStats = getSymbolStats()
    const isPanelOpen = showAllRanking || selectedBucket || selectedSymbol || selectedTimeBucket
    const timeDistData = getTimeDistributionData()

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
                    <button
                        className="reset-btn"
                        onClick={resetToDefaults}
                        title="重置为默认设置"
                        style={{ marginLeft: '4px' }}
                    >
                        ↺
                    </button>

                    {/* 搜索框 */}
                    <div className="header-search" style={{ marginLeft: '16px', position: 'relative' }}>
                        <input
                            type="text"
                            className="header-search-input"
                            placeholder="搜索币种..."
                            value={searchSymbol}
                            onChange={(e) => setSearchSymbol(e.target.value)}
                            onKeyDown={handleSearchKeyDown}
                            style={{ paddingRight: searchSymbol ? '52px' : '32px' }}
                        />
                        {searchSymbol && (
                            <button
                                className="header-search-clear"
                                onClick={() => setSearchSymbol('')}
                                title="清除"
                                style={{
                                    position: 'absolute',
                                    right: '28px',
                                    top: '50%',
                                    transform: 'translateY(-50%)',
                                    background: 'none',
                                    border: 'none',
                                    cursor: 'pointer',
                                    color: '#94a3b8',
                                    fontSize: '12px',
                                    padding: '2px 4px',
                                    lineHeight: 1
                                }}
                            >
                                ✕
                            </button>
                        )}
                        <button
                            className="header-search-btn"
                            onClick={handleSearchSymbol}
                            title="搜索"
                            style={{
                                position: 'absolute',
                                right: '4px',
                                top: '50%',
                                transform: 'translateY(-50%)',
                                background: 'none',
                                border: 'none',
                                cursor: 'pointer',
                                color: '#94a3b8',
                                fontSize: '14px',
                                padding: '2px 4px'
                            }}
                        >
                            🔍
                        </button>
                    </div>
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
                    {/* 胜率分析卡片 */}
                    <div className="stat-item win-rate-card" style={{ borderLeft: '3px solid #ec4899' }}>
                        <span className="icon">🎯</span>
                        <div className="win-rate-content">
                            <div className="win-rate-input-row">
                                <input
                                    type="text"
                                    className="win-rate-input"
                                    value={inputWinRate}
                                    onChange={handleWinRateChange}
                                    onBlur={applyWinRate}
                                    onKeyDown={handleWinRateKeyDown}
                                    title="输入胜率（50-99）"
                                />
                                <span className="win-rate-label">%胜率</span>
                            </div>
                            {calculateWinRateUptrend() && (
                                <span className="value" style={{ color: '#ec4899' }}>
                                    +{calculateWinRateUptrend().uptrend}%
                                </span>
                            )}
                        </div>
                    </div>
                </div>
            )}

            {/* 直方图 + 排行榜 */}
            <div className="distribution-charts">
                <div className={`chart-section ${isPanelOpen ? 'with-panel' : ''}`}>
                    <div className="section-title">
                        单边涨幅分布
                        <span style={{ fontSize: '12px', color: '#64748b', marginLeft: '8px' }}>(保留{Math.round(keepRatio * 100)}%涨幅 或 横盘{noNewHighCandles}根K线 视为波段结束)</span>

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

                    {/* 波段启动时间分布图表 */}
                    <div className="section-title" style={{ marginTop: '20px' }}>
                        波段启动时间分布
                        <span style={{ fontSize: '12px', color: '#64748b', marginLeft: '8px' }}>
                            (涨幅≥{timeChartThreshold}% 的波段)
                        </span>
                        <span className="label" style={{ marginLeft: '12px' }}>阈值:</span>
                        <input
                            type="text"
                            className="threshold-input"
                            value={inputTimeChartThreshold}
                            onChange={handleTimeChartThresholdChange}
                            onBlur={applyTimeChartThreshold}
                            onKeyDown={handleTimeChartThresholdKeyDown}
                            style={{ width: '45px', textAlign: 'center', marginLeft: '4px' }}
                            title="只统计涨幅大于此值的波段"
                        />
                        <span style={{ color: '#94a3b8', marginLeft: '2px' }}>%</span>
                        <span className="label" style={{ marginLeft: '12px' }}>粒度:</span>
                        <select
                            className="time-select"
                            value={timeGranularity}
                            onChange={(e) => setTimeGranularity(e.target.value === 'auto' ? 'auto' : Number(e.target.value))}
                            style={{ marginLeft: '4px' }}
                        >
                            {TIME_GRANULARITY_OPTIONS.map(opt => (
                                <option key={opt.value} value={opt.value}>
                                    {opt.label}
                                </option>
                            ))}
                        </select>
                        {timeDistData && (
                            <span style={{ marginLeft: '12px', fontSize: '12px', color: '#10b981' }}>
                                共 {timeDistData.totalWaves} 个波段，粒度: {timeDistData.bucketLabel}
                            </span>
                        )}
                    </div>

                    {/* 时间分布统计卡片 */}
                    {timeDistData && (
                        <div className="distribution-stats" style={{ marginTop: '12px', marginBottom: '12px' }}>
                            <div className="stat-item" style={{ borderLeft: '3px solid #10b981' }}>
                                <span className="icon">📊</span>
                                <span className="label">波段总数</span>
                                <span className="value" style={{ color: '#10b981' }}>{timeDistData.totalWaves}</span>
                            </div>
                            <div className="stat-item" style={{ borderLeft: '3px solid #6366f1' }}>
                                <span className="icon">⏱️</span>
                                <span className="label">平均用时</span>
                                <span className="value" style={{ color: '#6366f1' }}>{timeDistData.avgDuration}</span>
                            </div>
                            <div className="stat-item" style={{ borderLeft: '3px solid #8b5cf6' }}>
                                <span className="icon">🏆</span>
                                <span className="label">最长用时</span>
                                <span className="value" style={{ color: '#8b5cf6' }}>{timeDistData.maxDuration}</span>
                            </div>
                            <div className="stat-item" style={{ borderLeft: '3px solid #ef4444' }}>
                                <span className="icon">📈</span>
                                <span className="label">平均涨幅</span>
                                <span className="value" style={{ color: '#ef4444' }}>+{timeDistData.avgUptrend}%</span>
                            </div>
                            <div className="stat-item" style={{ borderLeft: '3px solid #f59e0b' }}>
                                <span className="icon">🔥</span>
                                <span className="label">最热时段</span>
                                <span className="value" style={{ color: '#f59e0b', fontSize: '0.85rem' }}>{timeDistData.hottestPeriod}</span>
                                <span className="percent">({timeDistData.hottestCount}个)</span>
                            </div>
                        </div>
                    )}

                    {timeDistData ? (
                        <ReactECharts
                            ref={timeChartRef}
                            option={getTimeDistributionOption()}
                            style={{ height: '280px', width: '100%' }}
                            opts={{ renderer: 'canvas' }}
                            onEvents={{ click: onTimeChartClick }}
                        />
                    ) : (
                        <div className="chart-loading" style={{ height: '100px' }}>
                            {uptrendData ? `暂无涨幅≥${timeChartThreshold}%的波段` : '加载中...'}
                        </div>
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
                                {selectedSymbol ? (
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
                                ) : (
                                    <div style={{ width: '32px' }} /> /* 占位保持对称 */
                                )}
                                <div className="ranking-title">
                                    <span className={`range-badge ${showAllRanking ? 'all' : ''}`} style={{ background: 'linear-gradient(135deg, #ef4444, #f59e0b)' }}>{rankingData.title}</span>
                                    <span className="coin-count">{rankingData.subtitle}</span>
                                </div>
                                <button className="close-btn" onClick={closePanel}>✕</button>
                            </div>
                            {/* 币种统计卡片 - 只在查看单个币种时显示 */}
                            {selectedSymbol && symbolStats && (
                                <div className="symbol-stats-cards">
                                    <div className="symbol-stat-card" style={{ borderLeftColor: '#ef4444' }}>
                                        <span className="stat-icon">📊</span>
                                        <div className="stat-content">
                                            <span className="stat-label">波段总数</span>
                                            <span className="stat-value" style={{ color: '#ef4444' }}>{symbolStats.totalWaves}</span>
                                        </div>
                                    </div>
                                    <div className="symbol-stat-card" style={{ borderLeftColor: '#10b981' }}>
                                        <span className="stat-icon">📈</span>
                                        <div className="stat-content">
                                            <span className="stat-label">平均涨幅</span>
                                            <span className="stat-value" style={{ color: '#10b981' }}>+{symbolStats.avgUptrend}%</span>
                                        </div>
                                    </div>
                                    <div className="symbol-stat-card" style={{ borderLeftColor: '#6366f1' }}>
                                        <span className="stat-icon">⏱️</span>
                                        <div className="stat-content">
                                            <span className="stat-label">平均用时</span>
                                            <span className="stat-value" style={{ color: '#6366f1' }}>{symbolStats.avgDuration}</span>
                                        </div>
                                    </div>
                                </div>
                            )}
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
                                    <button
                                        className={`sort-type-btn ${sortBy === 'duration' ? 'active' : ''}`}
                                        onClick={() => setSortBy('duration')}
                                        title="按波段持续时间排序"
                                    >
                                        ⏱️用时
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
                            <div className="ranking-list" ref={listRef} onScroll={handleListScroll}>
                                {rankingData.coins.slice(0, displayCount).map((coin, index) => (
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
                                                <span className="duration-badge">⏱{formatDuration(coin.waveStartTime, coin.waveEndTime)}</span>
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
                                {displayCount < rankingData.coins.length && (
                                    <div className="load-more-hint">
                                        ↓ 滚动加载更多 ({displayCount}/{rankingData.coins.length})
                                    </div>
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
