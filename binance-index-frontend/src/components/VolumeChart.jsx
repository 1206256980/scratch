import ReactECharts from 'echarts-for-react'

function VolumeChart({ data }) {
    if (!data || data.length === 0) {
        return null
    }

    // 处理数据 - 将成交额转换为亿为单位
    const chartData = data
        .filter(item => item.timestamp && item.totalVolume !== null && item.totalVolume !== undefined)
        .map(item => [item.timestamp, item.totalVolume / 100000000]) // 转换为亿

    const option = {
        backgroundColor: 'transparent',
        tooltip: {
            trigger: 'axis',
            backgroundColor: 'rgba(22, 27, 34, 0.95)',
            borderColor: 'rgba(245, 158, 11, 0.3)',
            borderWidth: 1,
            textStyle: {
                color: '#f1f5f9'
            },
            formatter: function (params) {
                if (!params || !params[0]) return ''
                const point = params[0]
                const time = new Date(point.data[0]).toLocaleString('zh-CN')
                const value = point.data[1].toFixed(2)
                return `
                    <div style="padding: 8px;">
                        <div style="color: #94a3b8; margin-bottom: 8px;">${time}</div>
                        <div style="color: #f59e0b; font-size: 18px; font-weight: 600;">
                            ${value} 亿 USDT
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
                    color: 'rgba(245, 158, 11, 0.2)'
                }
            },
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
                formatter: '{value} 亿'
            },
            splitLine: {
                lineStyle: {
                    color: 'rgba(245, 158, 11, 0.1)'
                }
            }
        },
        dataZoom: [
            {
                type: 'inside',
                start: 0,
                end: 100
            },
            {
                type: 'slider',
                start: 0,
                end: 100,
                height: 30,
                bottom: 10,
                borderColor: 'rgba(245, 158, 11, 0.2)',
                backgroundColor: 'rgba(22, 27, 34, 0.8)',
                fillerColor: 'rgba(245, 158, 11, 0.2)',
                handleStyle: {
                    color: '#f59e0b'
                },
                textStyle: {
                    color: '#64748b'
                }
            }
        ],
        series: [
            {
                name: '成交额',
                type: 'bar',
                barWidth: '60%',
                itemStyle: {
                    color: {
                        type: 'linear',
                        x: 0,
                        y: 0,
                        x2: 0,
                        y2: 1,
                        colorStops: [
                            { offset: 0, color: 'rgba(245, 158, 11, 0.8)' },
                            { offset: 1, color: 'rgba(245, 158, 11, 0.3)' }
                        ]
                    }
                },
                data: chartData
            }
        ]
    }

    return (
        <ReactECharts
            option={option}
            style={{ height: '350px', width: '100%' }}
            opts={{ renderer: 'canvas' }}
            notMerge={true}
        />
    )
}

export default VolumeChart
