import ReactECharts from 'echarts-for-react'

function IndexChart({ data }) {
    if (!data || data.length === 0) {
        return null
    }

    const option = {
        backgroundColor: 'transparent',
        tooltip: {
            trigger: 'axis',
            backgroundColor: 'rgba(22, 27, 34, 0.95)',
            borderColor: 'rgba(99, 102, 241, 0.3)',
            borderWidth: 1,
            textStyle: {
                color: '#f1f5f9'
            },
            formatter: function (params) {
                const point = params[0]
                const time = new Date(point.data[0]).toLocaleString('zh-CN')
                const value = point.data[1].toFixed(4)
                const color = point.data[1] >= 0 ? '#10b981' : '#ef4444'
                return `
          <div style="padding: 8px;">
            <div style="color: #94a3b8; margin-bottom: 8px;">${time}</div>
            <div style="color: ${color}; font-size: 18px; font-weight: 600;">
              ${point.data[1] >= 0 ? '+' : ''}${value}%
            </div>
          </div>
        `
            }
        },
        grid: {
            left: '3%',
            right: '4%',
            bottom: '15%',
            top: '8%',
            containLabel: true
        },
        xAxis: {
            type: 'time',
            axisLine: {
                lineStyle: {
                    color: 'rgba(99, 102, 241, 0.2)'
                }
            },
            axisLabel: {
                color: '#64748b',
                formatter: function (value) {
                    const date = new Date(value)
                    const hours = date.getHours().toString().padStart(2, '0')
                    const minutes = date.getMinutes().toString().padStart(2, '0')
                    const month = (date.getMonth() + 1).toString().padStart(2, '0')
                    const day = date.getDate().toString().padStart(2, '0')
                    return `${month}-${day}\n${hours}:${minutes}`
                }
            },
            splitLine: {
                show: false
            }
        },
        yAxis: {
            type: 'value',
            axisLine: {
                show: false
            },
            axisLabel: {
                color: '#64748b',
                formatter: '{value}%'
            },
            splitLine: {
                lineStyle: {
                    color: 'rgba(99, 102, 241, 0.1)'
                }
            }
        },
        dataZoom: [
            {
                type: 'inside',
                start: 0,
                end: 100,
                minValueSpan: 3600 * 1000 * 2 // 最小2小时
            },
            {
                type: 'slider',
                start: 0,
                end: 100,
                height: 30,
                bottom: 10,
                borderColor: 'rgba(99, 102, 241, 0.2)',
                backgroundColor: 'rgba(22, 27, 34, 0.8)',
                fillerColor: 'rgba(99, 102, 241, 0.2)',
                handleStyle: {
                    color: '#6366f1'
                },
                textStyle: {
                    color: '#64748b'
                },
                dataBackground: {
                    lineStyle: {
                        color: 'rgba(99, 102, 241, 0.5)'
                    },
                    areaStyle: {
                        color: 'rgba(99, 102, 241, 0.1)'
                    }
                }
            }
        ],
        visualMap: {
            show: false,
            pieces: [
                { lte: 0, color: '#ef4444' },
                { gt: 0, color: '#10b981' }
            ]
        },
        series: [
            {
                name: '市场指数',
                type: 'line',
                smooth: true,
                symbol: 'none',
                sampling: 'lttb',
                lineStyle: {
                    width: 2
                },
                areaStyle: {
                    color: {
                        type: 'linear',
                        x: 0,
                        y: 0,
                        x2: 0,
                        y2: 1,
                        colorStops: [
                            { offset: 0, color: 'rgba(99, 102, 241, 0.4)' },
                            { offset: 1, color: 'rgba(99, 102, 241, 0.05)' }
                        ]
                    }
                },
                data: data.map(item => [
                    new Date(item.timestamp).getTime(),
                    item.indexValue
                ]),
                markLine: {
                    silent: true,
                    symbol: 'none',
                    lineStyle: {
                        color: '#64748b',
                        type: 'dashed',
                        width: 1
                    },
                    data: [
                        { yAxis: 0, label: { show: true, formatter: '0%', color: '#64748b' } }
                    ]
                }
            }
        ]
    }

    return (
        <ReactECharts
            option={option}
            style={{ height: '450px', width: '100%' }}
            opts={{ renderer: 'canvas' }}
        />
    )
}

export default IndexChart
