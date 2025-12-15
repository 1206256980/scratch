import ReactECharts from 'echarts-for-react'

function CombinedChart({ data }) {
    if (!data || data.length === 0) {
        return null
    }

    // 处理指数数据
    const indexData = data
        .filter(item => item.timestamp && item.indexValue !== null && item.indexValue !== undefined)
        .map(item => [item.timestamp, item.indexValue])

    // 处理涨跌数据
    const upData = data
        .filter(item => item.timestamp && item.upCount !== null)
        .map(item => [item.timestamp, item.upCount || 0])

    const downData = data
        .filter(item => item.timestamp && item.downCount !== null)
        .map(item => [item.timestamp, item.downCount || 0])

    // 处理成交额数据 - 转换为亿
    const volumeData = data
        .filter(item => item.timestamp && item.totalVolume !== null && item.totalVolume !== undefined)
        .map(item => [item.timestamp, item.totalVolume / 100000000])

    const option = {
        backgroundColor: 'transparent',
        tooltip: {
            trigger: 'axis',
            axisPointer: {
                type: 'cross',
                link: [{ xAxisIndex: 'all' }],
                crossStyle: { color: '#64748b' }
            },
            backgroundColor: 'rgba(22, 27, 34, 0.95)',
            borderColor: 'rgba(99, 102, 241, 0.3)',
            borderWidth: 1,
            textStyle: { color: '#f1f5f9' },
            formatter: function (params) {
                if (!params || params.length === 0) return ''
                const time = new Date(params[0].data[0]).toLocaleString('zh-CN')

                let html = `<div style="padding: 8px;"><div style="color: #94a3b8; margin-bottom: 8px;">${time}</div>`

                params.forEach(param => {
                    if (param.seriesName === '市场指数') {
                        const value = param.data[1].toFixed(4)
                        const color = param.data[1] >= 0 ? '#10b981' : '#ef4444'
                        html += `<div style="display: flex; align-items: center; margin: 4px 0;">
                            <span style="display: inline-block; width: 10px; height: 10px; background: #6366f1; border-radius: 50%; margin-right: 8px;"></span>
                            <span style="color: #94a3b8; margin-right: 8px;">指数:</span>
                            <span style="color: ${color}; font-weight: 600;">${param.data[1] >= 0 ? '+' : ''}${value}%</span>
                        </div>`
                    } else if (param.seriesName === '上涨') {
                        html += `<div style="display: flex; align-items: center; margin: 4px 0;">
                            <span style="display: inline-block; width: 10px; height: 10px; background: #10b981; border-radius: 50%; margin-right: 8px;"></span>
                            <span style="color: #94a3b8; margin-right: 8px;">上涨:</span>
                            <span style="color: #10b981; font-weight: 600;">${param.data[1]}</span>
                        </div>`
                    } else if (param.seriesName === '下跌') {
                        html += `<div style="display: flex; align-items: center; margin: 4px 0;">
                            <span style="display: inline-block; width: 10px; height: 10px; background: #ef4444; border-radius: 50%; margin-right: 8px;"></span>
                            <span style="color: #94a3b8; margin-right: 8px;">下跌:</span>
                            <span style="color: #ef4444; font-weight: 600;">${param.data[1]}</span>
                        </div>`
                    } else if (param.seriesName === '成交额') {
                        const value = param.data[1].toFixed(2)
                        html += `<div style="display: flex; align-items: center; margin: 4px 0;">
                            <span style="display: inline-block; width: 10px; height: 10px; background: #f59e0b; border-radius: 50%; margin-right: 8px;"></span>
                            <span style="color: #94a3b8; margin-right: 8px;">成交额:</span>
                            <span style="color: #f59e0b; font-weight: 600;">${value} 亿</span>
                        </div>`
                    }
                })

                html += '</div>'
                return html
            }
        },
        axisPointer: {
            link: [{ xAxisIndex: 'all' }]
        },
        grid: [
            {
                left: '3%',
                right: '4%',
                top: '6%',
                height: '28%',
                containLabel: true
            },
            {
                left: '3%',
                right: '4%',
                top: '42%',
                height: '16%',
                containLabel: true
            },
            {
                left: '3%',
                right: '4%',
                top: '68%',
                height: '20%',
                containLabel: true
            }
        ],
        xAxis: [
            {
                type: 'time',
                gridIndex: 0,
                axisLine: { lineStyle: { color: 'rgba(99, 102, 241, 0.2)' } },
                axisLabel: { show: false },
                splitLine: { show: false },
                axisPointer: { show: true }
            },
            {
                type: 'time',
                gridIndex: 1,
                axisLine: { lineStyle: { color: 'rgba(245, 158, 11, 0.2)' } },
                axisLabel: { show: false },
                splitLine: { show: false },
                axisPointer: { show: true }
            },
            {
                type: 'time',
                gridIndex: 2,
                axisLine: { lineStyle: { color: 'rgba(100, 116, 139, 0.2)' } },
                axisLabel: {
                    color: '#64748b',
                    formatter: function (value) {
                        const date = new Date(value)
                        const month = (date.getMonth() + 1).toString().padStart(2, '0')
                        const day = date.getDate().toString().padStart(2, '0')
                        const hours = date.getHours().toString().padStart(2, '0')
                        const minutes = date.getMinutes().toString().padStart(2, '0')
                        return `${month}-${day}\n${hours}:${minutes}`
                    }
                },
                splitLine: { show: false },
                axisPointer: { show: true }
            }
        ],
        yAxis: [
            {
                type: 'value',
                gridIndex: 0,
                name: '指数 %',
                nameTextStyle: { color: '#64748b' },
                axisLine: { show: false },
                axisLabel: { color: '#64748b', formatter: '{value}%' },
                splitLine: { lineStyle: { color: 'rgba(99, 102, 241, 0.1)' } }
            },
            {
                type: 'value',
                gridIndex: 1,
                name: '成交额 (亿)',
                nameTextStyle: { color: '#64748b' },
                axisLine: { show: false },
                axisLabel: { color: '#64748b' },
                splitLine: { lineStyle: { color: 'rgba(245, 158, 11, 0.1)' } }
            },
            {
                type: 'value',
                gridIndex: 2,
                name: '涨/跌数',
                nameTextStyle: { color: '#64748b' },
                axisLine: { show: false },
                axisLabel: { color: '#64748b' },
                splitLine: { lineStyle: { color: 'rgba(100, 116, 139, 0.1)' } }
            }
        ],
        dataZoom: [
            {
                type: 'inside',
                xAxisIndex: [0, 1, 2],
                start: 0,
                end: 100
            },
            {
                type: 'slider',
                xAxisIndex: [0, 1, 2],
                start: 0,
                end: 100,
                height: 50,
                bottom: 10,
                borderColor: 'rgba(99, 102, 241, 0.2)',
                backgroundColor: 'rgba(22, 27, 34, 0.8)',
                fillerColor: 'rgba(99, 102, 241, 0.2)',
                handleStyle: { color: '#6366f1' },
                textStyle: { color: '#64748b' }
            }
        ],
        series: [
            {
                name: '市场指数',
                type: 'line',
                xAxisIndex: 0,
                yAxisIndex: 0,
                smooth: true,
                showSymbol: false,
                lineStyle: { width: 2, color: '#6366f1' },
                areaStyle: {
                    color: {
                        type: 'linear',
                        x: 0, y: 0, x2: 0, y2: 1,
                        colorStops: [
                            { offset: 0, color: 'rgba(99, 102, 241, 0.4)' },
                            { offset: 1, color: 'rgba(99, 102, 241, 0.05)' }
                        ]
                    }
                },
                data: indexData,
                markLine: {
                    silent: true,
                    symbol: 'none',
                    lineStyle: { color: '#64748b', type: 'dashed', width: 1 },
                    data: [{ yAxis: 0, label: { show: true, formatter: '0%', color: '#64748b' } }]
                }
            },
            {
                name: '成交额',
                type: 'line',
                xAxisIndex: 1,
                yAxisIndex: 1,
                smooth: true,
                symbol: 'none',
                lineStyle: { width: 2, color: '#f59e0b' },
                areaStyle: {
                    color: {
                        type: 'linear',
                        x: 0, y: 0, x2: 0, y2: 1,
                        colorStops: [
                            { offset: 0, color: 'rgba(245, 158, 11, 0.4)' },
                            { offset: 1, color: 'rgba(245, 158, 11, 0.05)' }
                        ]
                    }
                },
                data: volumeData
            },
            {
                name: '上涨',
                type: 'line',
                xAxisIndex: 2,
                yAxisIndex: 2,
                smooth: true,
                showSymbol: false,
                lineStyle: { width: 2, color: '#10b981' },
                data: upData
            },
            {
                name: '下跌',
                type: 'line',
                xAxisIndex: 2,
                yAxisIndex: 2,
                smooth: true,
                showSymbol: false,
                lineStyle: { width: 2, color: '#ef4444' },
                data: downData
            }
        ]
    }

    return (
        <ReactECharts
            option={option}
            style={{ height: '620px', width: '100%' }}
            opts={{ renderer: 'canvas' }}
            notMerge={true}
        />
    )
}

export default CombinedChart
