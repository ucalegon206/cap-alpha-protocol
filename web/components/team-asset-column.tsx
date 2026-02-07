"use client"

import * as React from "react"
import { getTradeableAssets, getTeams } from "@/app/actions"
import { ScrollArea } from "@/components/ui/scroll-area"
import { DraggablePlayerCard } from "./draggable-player-card"
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "@/components/ui/select"
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card"

export function TeamAssetColumn({ title, onTeamChange }: { title: string, onTeamChange?: (team: string) => void }) {
    const [teams, setTeams] = React.useState<string[]>([])
    const [selectedTeam, setSelectedTeam] = React.useState<string>("")
    const [assets, setAssets] = React.useState<any[]>([])

    React.useEffect(() => {
        getTeams().then(setTeams)
    }, [])

    React.useEffect(() => {
        if (selectedTeam) {
            getTradeableAssets(selectedTeam).then(setAssets)
            if (onTeamChange) onTeamChange(selectedTeam)
        }
    }, [selectedTeam, onTeamChange])

    return (
        <Card className="flex flex-col h-[600px] border-border bg-card/50">
            <CardHeader className="pb-3">
                <CardTitle className="text-sm font-mono uppercase tracking-widest text-muted-foreground">{title}</CardTitle>
                <Select onValueChange={setSelectedTeam} value={selectedTeam}>
                    <SelectTrigger className="w-full bg-background mt-2">
                        <SelectValue placeholder="Select Team..." />
                    </SelectTrigger>
                    <SelectContent>
                        {teams.map((team) => (
                            <SelectItem key={team} value={team}>{team}</SelectItem>
                        ))}
                    </SelectContent>
                </Select>
            </CardHeader>
            <CardContent className="flex-1 overflow-hidden p-0">
                <ScrollArea className="h-full px-4 pb-4">
                    {assets.length > 0 ? (
                        assets.map((asset) => (
                            <DraggablePlayerCard key={asset.id} player={asset} />
                        ))
                    ) : (
                        <div className="text-center py-20 text-muted-foreground text-sm italic">
                            {selectedTeam ? "No tradeable assets found." : "Select a team to view assets."}
                        </div>
                    )}
                </ScrollArea>
            </CardContent>
        </Card>
    )
}
