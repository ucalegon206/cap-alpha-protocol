
"use client";

import React from "react";
import { usePersona, Persona } from "./persona-context";
import { Activity, CircleDollarSign, Users } from "lucide-react";
import { cn } from "@/lib/utils";

export default function PersonaSwitcher() {
    const { persona, setPersona } = usePersona();

    const options: { id: Persona; label: string; icon: React.ReactNode }[] = [
        { id: "FAN", label: "Fan", icon: <Users className="h-4 w-4" /> },
        { id: "BETTOR", label: "Bettor", icon: <Activity className="h-4 w-4" /> },
        { id: "AGENT", label: "Agent", icon: <CircleDollarSign className="h-4 w-4" /> },
    ];

    return (
        <div className="flex items-center space-x-1 bg-gray-900/50 backdrop-blur-md p-1 rounded-lg border border-white/10">
            {options.map((option) => (
                <button
                    key={option.id}
                    onClick={() => setPersona(option.id)}
                    className={cn(
                        "flex items-center space-x-2 px-3 py-1.5 rounded-md text-sm font-medium transition-all duration-200",
                        persona === option.id
                            ? "bg-blue-600 text-white shadow-lg shadow-blue-500/20"
                            : "text-gray-400 hover:text-white hover:bg-white/5"
                    )}
                >
                    {option.icon}
                    <span>{option.label}</span>
                </button>
            ))}
        </div>
    );
}
