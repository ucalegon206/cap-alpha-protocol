'use client';

import { ResponsiveContainer, ScatterChart, Scatter, XAxis, YAxis, ZAxis, Tooltip, ReferenceLine, Cell } from 'recharts';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

interface EfficiencyLandscapeProps {
    data: {
        player_name: string;
        team: string;
        position: string;
        cap_hit_millions: number;
        surplus_value: number; // FMV
        risk_score: number;
    }[];
}

export function EfficiencyLandscape({ data }: EfficiencyLandscapeProps) {
    // Filter for significant players to avoid noise
    const chartData = data
        .filter(d => d.cap_hit_millions > 2)
        .map(d => ({
            ...d,
            // Production = FMV
            // Cost = Cap Hit
            efficiency: (d.surplus_value - d.cap_hit_millions).toFixed(1),
        }));

    return (
        <Card className="col-span-4 bg-background border-border shadow-md">
            <CardHeader>
                <div className="flex justify-between items-start">
                    <div>
                        <CardTitle className="text-xl">Market Efficiency Landscape</CardTitle>
                        <CardDescription>
                            Comparing <span className="text-emerald-500 font-bold">Production (FMV)</span> vs. <span className="text-rose-500 font-bold">Cost (Cap Hit)</span>.
                            Top-left is "Surplus Value" (Good). Bottom-right is "Dead Weight" (Bad).
                        </CardDescription>
                    </div>
                    <Badge variant="outline" className="font-mono">N={chartData.length}</Badge>
                </div>
            </CardHeader>
            <CardContent className="h-[400px] w-full p-0">
                <ResponsiveContainer width="100%" height="100%">
                    <ScatterChart margin={{ top: 20, right: 20, bottom: 20, left: 20 }}>
                        {/* Quadrant Lines */}
                        <ReferenceLine y={20} stroke="hsl(var(--muted-foreground))" strokeDasharray="3 3" label={{ value: 'Elite Cost Threshold ($20M)', position: 'insideBottomRight', fill: 'gray', fontSize: 10 }} />
                        <ReferenceLine x={20} stroke="hsl(var(--muted-foreground))" strokeDasharray="3 3" />

                        <XAxis
                            type="number"
                            dataKey="cap_hit_millions"
                            name="Cost"
                            unit="M"
                            label={{ value: 'Examples: Deshaun Watson (High Cost)', position: 'insideBottom', offset: -10, fill: 'gray', fontSize: 12 }}
                            domain={[0, 'auto']}
                        />
                        <YAxis
                            type="number"
                            dataKey="surplus_value"
                            name="Production"
                            unit="M"
                            label={{ value: 'Examples: Brock Purdy (High Value)', angle: -90, position: 'insideLeft', fill: 'gray', fontSize: 12 }}
                            domain={[0, 'auto']}
                        />
                        <ZAxis type="number" dataKey="risk_score" range={[50, 400]} name="Risk" />

                        <Tooltip
                            cursor={{ strokeDasharray: '3 3' }}
                            content={({ active, payload }) => {
                                if (active && payload && payload.length) {
                                    const d = payload[0].payload;
                                    return (
                                        <div className="rounded-lg border bg-popover p-3 shadow-sm">
                                            <div className="flex flex-col gap-1">
                                                <span className="font-bold text-sm uppercase">{d.player_name}</span>
                                                <div className="flex justify-between gap-4 text-xs">
                                                    <span className="text-muted-foreground">{d.team} • {d.position}</span>
                                                    <span className={Number(d.efficiency) > 0 ? "text-emerald-500" : "text-rose-500"}>
                                                        {Number(d.efficiency) > 0 ? "+" : ""}{d.efficiency}M Value
                                                    </span>
                                                </div>
                                                <div className="grid grid-cols-2 gap-2 mt-2 text-[10px] text-muted-foreground">
                                                    <div>Cost: ${d.cap_hit_millions}M</div>
                                                    <div>FMV: ${d.surplus_value.toFixed(1)}M</div>
                                                </div>
                                            </div>
                                        </div>
                                    );
                                }
                                return null;
                            }}
                        />

                        <Scatter name="Players" data={chartData} fill="#8884d8">
                            {chartData.map((entry, index) => (
                                <Cell
                                    key={`cell-${index}`}
                                    fill={entry.risk_score > 0.7 ? '#f43f5e' : entry.risk_score > 0.4 ? '#fbbf24' : '#10b981'}
                                    fillOpacity={0.7}
                                />
                            ))}
                        </Scatter>
                    </ScatterChart>
                </ResponsiveContainer>
            </CardContent>
        </Card>
    );
}
