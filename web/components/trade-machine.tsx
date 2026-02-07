"use client"

import * as React from "react"
import { DndContext, DragEndEvent, useSensor, useSensors, PointerSensor, KeyboardSensor, MeasuringStrategy } from "@dnd-kit/core"
import { TeamAssetColumn } from "./team-asset-column"
import { TradeSimulationZone } from "./trade-simulation-zone"
import { simulateTrade } from "@/app/actions"
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Info } from "lucide-react"

export function TradeMachine() {
    const [teamA, setTeamA] = React.useState("")
    const [teamB, setTeamB] = React.useState("")
    const [assetsA, setAssetsA] = React.useState<any[]>([])
    const [assetsB, setAssetsB] = React.useState<any[]>([])
    const [simulationResult, setSimulationResult] = React.useState<any>(null)

    const sensors = useSensors(
        useSensor(PointerSensor, {
            activationConstraint: {
                distance: 8,
            },
        }),
        useSensor(KeyboardSensor)
    )

    const handleDragEnd = (event: DragEndEvent) => {
        const { active, over } = event

        if (!over) return

        const asset = active.data.current as any
        const dropZone = over.id as string

        if (asset && dropZone === 'team-a-trade-zone' && asset.team === teamA) {
            if (!assetsA.find(a => a.id === asset.id)) {
                setAssetsA([...assetsA, asset])
            }
        } else if (asset && dropZone === 'team-b-trade-zone' && asset.team === teamB) {
            if (!assetsB.find(a => a.id === asset.id)) {
                setAssetsB([...assetsB, asset])
            }
        }
    }

    const removeAsset = (id: string, team: 'A' | 'B') => {
        if (team === 'A') {
            setAssetsA(assetsA.filter(a => a.id !== id))
        } else {
            setAssetsB(assetsB.filter(a => a.id !== id))
        }
    }

    const handleSimulate = async () => {
        const result = await simulateTrade([...assetsA, ...assetsB])
        setSimulationResult(result)
    }

    return (
        <DndContext
            sensors={sensors}
            onDragEnd={handleDragEnd}
            measuring={{
                droppable: {
                    strategy: MeasuringStrategy.Always
                }
            }}
        >
            <div className="space-y-6">
                <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                    {/* Left Column: Team A Assets */}
                    <div className="lg:col-span-1">
                        <TeamAssetColumn title="Team A Roster" onTeamChange={setTeamA} />
                    </div>

                    {/* Center Column: Simulation Zone */}
                    <div className="lg:col-span-1 space-y-6">
                        <div className="bg-emerald-500/5 border border-emerald-500/20 rounded-lg p-4 mb-4">
                            <div className="flex gap-2">
                                <Info className="h-4 w-4 text-emerald-500 mt-0.5" />
                                <p className="text-xs text-muted-foreground">
                                    Drag players from either team into their respective exchange zones to simulate the trade impact.
                                </p>
                            </div>
                        </div>
                        <TradeSimulationZone
                            teamA={teamA}
                            teamB={teamB}
                            assetsA={assetsA}
                            assetsB={assetsB}
                            onRemoveAsset={removeAsset}
                            onSimulate={handleSimulate}
                        />

                        {simulationResult && (
                            <Card className="border-emerald-500 animate-in fade-in slide-in-from-bottom-4 duration-500">
                                <CardHeader className="pb-2">
                                    <CardTitle className="text-sm font-mono text-emerald-500 uppercase">Simulation Report</CardTitle>
                                </CardHeader>
                                <CardContent>
                                    <p className="text-sm text-foreground mb-4">{simulationResult.summary}</p>
                                    <div className="grid grid-cols-2 gap-4">
                                        <div className="p-3 bg-secondary rounded-md">
                                            <p className="text-[10px] text-muted-foreground uppercase">Win Prob Δ</p>
                                            <p className="text-lg font-bold text-emerald-500">+{(simulationResult.win_prob_delta * 100).toFixed(1)}%</p>
                                        </div>
                                        <div className="p-3 bg-secondary rounded-md">
                                            <p className="text-[10px] text-muted-foreground uppercase">Efficiency Score</p>
                                            <p className="text-lg font-bold text-emerald-500">A+</p>
                                        </div>
                                    </div>
                                </CardContent>
                            </Card>
                        )}
                    </div>

                    {/* Right Column: Team B Assets */}
                    <div className="lg:col-span-1">
                        <TeamAssetColumn title="Team B Roster" onTeamChange={setTeamB} />
                    </div>
                </div>
            </div>
        </DndContext>
    )
}
