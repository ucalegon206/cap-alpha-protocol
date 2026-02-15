
"use client";

import { PlayerEfficiency } from "@/app/actions";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { ArrowLeft, TrendingUp, TrendingDown, DollarSign, AlertTriangle } from "lucide-react";
import Link from "next/link";
import {
    ComposedChart,
    Line,
    Bar,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    Legend,
    ResponsiveContainer,
    Area
} from "recharts";

export default function PlayerDetailView({ player }: { player: PlayerEfficiency }) {
    // Calculate Cumulative Error
    const history = player.history || [];
    const cumulativeError = history.reduce((acc, curr) => acc + (curr.actual - curr.predicted), 0);

    // Determine status color
    const statusColor = cumulativeError > 10 ? "text-red-500" : cumulativeError < -5 ? "text-emerald-500" : "text-amber-500";
    const statusText = cumulativeError > 10 ? "OVERPAID" : cumulativeError < -5 ? "SURPLUS VALUE" : "FAIR VALUE";

    return (
        <div className="max-w-7xl mx-auto space-y-8">
            {/* Header / Nav */}
            <div className="flex items-center space-x-4">
                <Link href="/" className="p-2 hover:bg-zinc-800 rounded-full transition-colors">
                    <ArrowLeft className="w-6 h-6 text-zinc-400" />
                </Link>
                <div>
                    <h1 className="text-4xl font-bold tracking-tight">{player.player_name}</h1>
                    <div className="flex items-center space-x-2 text-zinc-400 mt-1">
                        <span className="font-semibold text-white">{player.team}</span>
                        <span>•</span>
                        <span>{player.position}</span>
                        <span>•</span>
                        <span>Age {player.age}</span>
                    </div>
                </div>
                <div className="ml-auto flex items-center space-x-4">
                    <div className="text-right">
                        <div className="text-sm text-zinc-500 uppercase tracking-widest">Efficiency Rating</div>
                        <div className={`text-2xl font-bold ${statusColor}`}>{statusText}</div>
                    </div>
                    <div className={`p-4 rounded-xl border ${cumulativeError > 0 ? 'bg-red-500/10 border-red-500/20' : 'bg-emerald-500/10 border-emerald-500/20'}`}>
                        <div className="text-xs text-zinc-500">Cumulative Error</div>
                        <div className={`text-xl font-mono font-bold ${cumulativeError > 0 ? 'text-red-500' : 'text-emerald-500'}`}>
                            {cumulativeError > 0 ? '+' : ''}{cumulativeError.toFixed(2)}M
                        </div>
                    </div>
                </div>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                {/* Main Chart: The "Money Chart" */}
                <Card className="lg:col-span-2 bg-zinc-900 border-zinc-800">
                    <CardHeader>
                        <CardTitle>Cap Hit Trajectory vs. Model Prediction</CardTitle>
                        <CardDescription>
                            Historical analysis of actual cap hits against the Fair Market Value (FMV) model.
                        </CardDescription>
                    </CardHeader>
                    <CardContent className="h-[400px]">
                        <ResponsiveContainer width="100%" height="100%">
                            <ComposedChart data={history}>
                                <defs>
                                    <linearGradient id="colorPredicted" x1="0" y1="0" x2="0" y2="1">
                                        <stop offset="5%" stopColor="#10b981" stopOpacity={0.3} />
                                        <stop offset="95%" stopColor="#10b981" stopOpacity={0} />
                                    </linearGradient>
                                </defs>
                                <CartesianGrid strokeDasharray="3 3" stroke="#27272a" vertical={false} />
                                <XAxis
                                    dataKey="year"
                                    stroke="#71717a"
                                    tickLine={false}
                                    axisLine={false}
                                />
                                <YAxis
                                    stroke="#71717a"
                                    tickFormatter={(value) => `$${value}M`}
                                    tickLine={false}
                                    axisLine={false}
                                />
                                <Tooltip
                                    contentStyle={{ backgroundColor: '#18181b', borderColor: '#27272a', color: '#fff' }}
                                    itemStyle={{ color: '#fff' }}
                                    formatter={(value: any) => [`$${Number(value).toFixed(2)}M`, '']}
                                />
                                <Legend wrapperStyle={{ paddingTop: '20px' }} />
                                <Bar
                                    dataKey="actual"
                                    name="Actual Cap Hit"
                                    barSize={20}
                                    fill="#3f3f46"
                                    radius={[4, 4, 0, 0]}
                                />
                                <Area
                                    type="monotone"
                                    dataKey="predicted"
                                    name="Model Fair Value"
                                    fill="url(#colorPredicted)"
                                    stroke="#10b981"
                                    strokeWidth={3}
                                />
                            </ComposedChart>
                        </ResponsiveContainer>
                    </CardContent>
                </Card>

                {/* Efficiency Stats Card */}
                <div className="space-y-6">
                    <Card className="bg-zinc-900 border-zinc-800">
                        <CardHeader>
                            <CardTitle>Risk Profile</CardTitle>
                        </CardHeader>
                        <CardContent className="space-y-4">
                            <div className="flex justify-between items-center">
                                <span className="text-zinc-400">Projected Risk Score</span>
                                <Badge variant="outline" className={player.risk_score > 0.7 ? "bg-red-500/10 text-red-500 border-red-500/20" : "bg-emerald-500/10 text-emerald-500 border-emerald-500/20"}>
                                    {(player.risk_score * 100).toFixed(1)}/100
                                </Badge>
                            </div>
                            <Separator className="bg-zinc-800" />
                            <div className="flex justify-between items-center">
                                <span className="text-zinc-400">Current Cap Hit</span>
                                <span className="font-mono">${player.cap_hit_millions.toFixed(2)}M</span>
                            </div>
                            <div className="flex justify-between items-center">
                                <span className="text-zinc-400">Dead Cap Liability</span>
                                <span className="font-mono text-amber-500">${player.dead_cap_millions.toFixed(2)}M</span>
                            </div>
                            <Separator className="bg-zinc-800" />
                            <div className="pt-2">
                                <div className="text-xs text-zinc-500 mb-2">INTELLIGENCE NOTE</div>
                                <p className="text-sm text-zinc-300 leading-relaxed">
                                    {player.risk_score > 0.7
                                        ? "Critical Risk Asset. Model indicates significant overpayment relative to performance production. Recommended action: Restructure or cut post-June 1."
                                        : "Stable Asset. Contract value aligns with production metrics. Retain at current APY."}
                                </p>
                            </div>
                        </CardContent>
                    </Card>

                    {/* Raw Data Table (Condensed) */}
                    <Card className="bg-zinc-900 border-zinc-800">
                        <CardHeader>
                            <CardTitle>Historical Ledger</CardTitle>
                        </CardHeader>
                        <CardContent>
                            <div className="space-y-2">
                                {history.slice().reverse().slice(0, 5).map((h) => (
                                    <div key={h.year} className="flex justify-between items-center text-sm p-2 hover:bg-white/5 rounded">
                                        <span className="font-mono text-zinc-500">{h.year}</span>
                                        <div className="flex space-x-4">
                                            <span className="text-zinc-300">${h.actual.toFixed(1)}M</span>
                                            <span className={h.actual > h.predicted ? "text-red-500" : "text-emerald-500"}>
                                                {h.actual > h.predicted ? '+' : ''}{(h.actual - h.predicted).toFixed(1)}M
                                            </span>
                                        </div>
                                    </div>
                                ))}
                            </div>
                        </CardContent>
                    </Card>
                </div>
            </div>
        </div>
    );
}
