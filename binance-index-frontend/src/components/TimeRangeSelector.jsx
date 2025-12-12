function TimeRangeSelector({ value, onChange }) {
    const options = [
        { label: '6小时', hours: 6 },
        { label: '12小时', hours: 12 },
        { label: '1天', hours: 24 },
        { label: '3天', hours: 72 },
        { label: '7天', hours: 168 }
    ]

    return (
        <div className="time-range-selector">
            {options.map(option => (
                <button
                    key={option.hours}
                    className={value === option.hours ? 'active' : ''}
                    onClick={() => onChange(option.hours)}
                >
                    {option.label}
                </button>
            ))}
        </div>
    )
}

export default TimeRangeSelector
