function StatsCard({ label, value, valueClass = '', subValue = '' }) {
    return (
        <div className="stat-card">
            <div className="label">{label}</div>
            <div className={`value ${valueClass}`}>{value}</div>
            {subValue && <div className="sub-value">{subValue}</div>}
        </div>
    )
}

export default StatsCard
