"use client"

import { useDraggable } from "@dnd-kit/core"
import { Card, CardContent } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { GripVertical } from "lucide-react"
import { CapSparkline } from "./ui/cap-sparkline"
import { generateCapHistory } from "@/lib/utils"

export function DraggablePlayerCard({ player }: { player: any }) {
    const { attributes, listeners, setNodeRef, transform, isDragging } = useDraggable({
        id: player.id,
        data: {
            ...player
        }
    })

    const style = transform ? {
        transform: `translate3d(${transform.x}px, ${transform.y}px, 0)`,
        zIndex: isDragging ? 100 : 1,
        opacity: isDragging ? 0.5 : 1
    } : undefined

    return (
        <div ref={setNodeRef} style={style} {...listeners} {...attributes} className="mb-2 touch-none cursor-grab active:cursor-grabbing">
            <Card className="hover:border-emerald-500 transition-colors">
                <CardContent className="p-3 flex items-center justify-between">
                    <div className="flex items-center gap-3">
                        <GripVertical className="h-4 w-4 text-muted-foreground" />
                        <div>
                            <p className="font-bold text-sm uppercase">{player.name}</p>
                            <p className="text-xs text-muted-foreground">{player.position} • {player.team}</p>
                        </div>
                    </div>

                    {/* Tufte Sparkline: Cap History */}
                    <div className="hidden sm:block opacity-50 hover:opacity-100 transition-opacity">
                        <CapSparkline
                            data={generateCapHistory(player.cap_hit_millions, player.risk_score)}
                            width={48}
                            height={24}
                        />
                    </div>
                    <div className="text-right">
                        <p className="text-sm font-mono text-emerald-500">${player.cap_hit_millions}M</p>
                        <Badge variant={player.risk_score > 0.7 ? "destructive" : "outline"} className="text-[10px] py-0 px-1">
                            RISK: {player.risk_score.toFixed(2)}
                        </Badge>
                    </div>
                </CardContent>
            </Card>
        </div>
    )
}
