import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import Footer from "@/components/footer";
import { PersonaProvider } from "@/components/persona-context";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
    title: "Cap Alpha Protocol",
    description: "Advanced Roster Management System",
};

import { ClerkProvider } from '@clerk/nextjs'
import { dark } from '@clerk/themes'

export default function RootLayout({
    children,
}: Readonly<{
    children: React.ReactNode;
}>) {
    return (
        <ClerkProvider
            appearance={{
                baseTheme: dark,
                variables: { colorPrimary: '#10b981' }, // Emerald-500
            }}
        >
            <html lang="en">
                <body className={`${inter.className} min-h-screen flex flex-col`}>
                    <main className="flex-grow">
                        <PersonaProvider>
                            {children}
                        </PersonaProvider>
                    </main>
                    <Footer />
                </body>
            </html>
        </ClerkProvider>
    );
}
