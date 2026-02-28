import type { Metadata } from "next";
import { Inter } from "next/font/google";
import Link from "next/link";
import "./globals.css";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "BJJ Search",
  description: "Search across your BJJ instructional library by technique, position, or situation",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className="dark">
      <body className={inter.className}>
        <nav className="border-b border-gray-800 bg-gray-900/80 backdrop-blur-sm sticky top-0 z-50">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div className="flex items-center justify-between h-14">
              <Link href="/" className="flex items-center gap-2 text-lg font-bold text-white hover:text-bjj-400 transition-colors">
                <span className="text-bjj-500">BJJ</span> Search
              </Link>
              <div className="flex items-center gap-6">
                <Link href="/" className="text-sm text-gray-400 hover:text-white transition-colors">
                  Search
                </Link>
                <Link href="/browse" className="text-sm text-gray-400 hover:text-white transition-colors">
                  Browse
                </Link>
                <Link href="/library" className="text-sm text-gray-400 hover:text-white transition-colors">
                  Library
                </Link>
                <Link href="/ingest" className="text-sm text-gray-400 hover:text-white transition-colors">
                  Ingest
                </Link>
              </div>
            </div>
          </div>
        </nav>
        <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          {children}
        </main>
      </body>
    </html>
  );
}
