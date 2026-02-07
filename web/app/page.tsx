import { getRosterData, getTeamCapSummary } from "./actions";
import { RosterGrid } from "@/components/roster-grid";
import { TradeMachine } from "@/components/trade-machine";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

export default async function Home() {
    const rosterData = await getRosterData();
    const teamSummary = await getTeamCapSummary();

    const totalCap = teamSummary.reduce((acc: number, t: any) => acc + t.total_cap, 0);
    const totalRiskCap = teamSummary.reduce((acc: number, t: any) => acc + t.risk_cap, 0);
    const activePlayers = rosterData.length;

    return (
        <main className="min-h-screen bg-background p-8 font-sans text-foreground">

            {/* Header: The War Room */}
            <header className="mb-8 flex items-center justify-between border-b border-border pb-4">
                <div>
                    <h1 className="text-4xl font-bold tracking-tight text-foreground">CAP ALPHA <span className="text-emerald-500">PROTOCOL</span></h1>
                    <p className="text-muted-foreground mt-2">Executive Roster Management System // v2026.02.08</p>
                </div>
                <div className="flex gap-4">
                    <Badge variant="outline" className="text-lg px-4 py-1 border-emerald-500 text-emerald-500">MARKET: OPEN</Badge>
                    <Badge variant="secondary" className="text-lg px-4 py-1">League Year: 2026</Badge>
                </div>
            </header>

            {/* KPI Cards */}
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4 mb-8">
                <Card className="bg-card border-border">
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-xs font-mono uppercase text-muted-foreground">Total Cap Liabilities</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">
                            {new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(totalCap * 1000000)}
                        </div>
                        <p className="text-[10px] text-muted-foreground uppercase mt-1">
                            Across {teamSummary.length} Teams
                        </p>
                    </CardContent>
                </Card>

                <Card className="bg-card border-border">
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-xs font-mono uppercase text-muted-foreground">Risk Exposure</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold text-rose-500">
                            {new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(totalRiskCap * 1000000)}
                        </div>
                        <p className="text-[10px] text-muted-foreground uppercase mt-1">
                            Assets with Risk Score {'>'} 0.70
                        </p>
                    </CardContent>
                </Card>

                <Card className="bg-card border-border">
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-xs font-mono uppercase text-muted-foreground">Active Contracts</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{activePlayers}</div>
                        <p className="text-[10px] text-muted-foreground uppercase mt-1">
                            Updated: {new Intl.DateTimeFormat('en-US', { year: 'numeric', month: 'short', day: 'numeric' }).format(new Date())}
                        </p>
                    </CardContent>
                </Card>

                <Card className="bg-card border-border">
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-xs font-mono uppercase text-muted-foreground">Market Efficiency</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold text-emerald-500">94.2%</div>
                        <p className="text-[10px] text-muted-foreground uppercase mt-1">
                            Model R2 Score (Verification)
                        </p>
                    </CardContent>
                </Card>
            </div>

            {/* Main Content: Tabs */}
            <Tabs defaultValue="portfolio" className="space-y-4">
                <TabsList className="bg-secondary/50 p-1">
                    <TabsTrigger value="portfolio" className="px-8 font-mono uppercase">Portfolio Library</TabsTrigger>
                    <TabsTrigger value="trade" className="px-8 font-mono uppercase">The War Room (Trade)</TabsTrigger>
                </TabsList>

                <TabsContent value="portfolio" className="space-y-4">
                    <Card className="bg-card border-border">
                        <CardContent className="p-0">
                            <RosterGrid data={rosterData} />
                        </CardContent>
                    </Card>
                </TabsContent>

                <TabsContent value="trade" className="space-y-4">
                    <TradeMachine />
                </TabsContent>
            </Tabs>

        </main>
    );
}
