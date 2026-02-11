"use client"

import { LineChart, Line, ResponsiveContainer, YAxis } from 'recharts'

export function CapSparkline({ data, height = 24, width = 64, color }: { data: number[], height?: number, width?: number, color?: string }) {
    // Transform array to object array for Recharts
    const chartData = data.map((val, i) => ({ i, val }));

    // Determine color based on trend if not provided
    const isRising = data[data.length - 1] > data[0];
    const strokeColor = color || (isRising ? "#ef4444" : "#10b981"); // Red if cap hits go UP (bad for team), Green if down

    return (
        <div style={{ height, width }}>
            <ResponsiveContainer width="100%" height="100%">
                <LineChart data={chartData}>
                    <Line
                        type="monotone"
                        dataKey="val"
                        stroke={strokeColor}
                        strokeWidth={2}
                        dot={false}
                        isAnimationActive={false}
                    />
                </LineChart>
            </ResponsiveContainer>
        </div>
    )
}
