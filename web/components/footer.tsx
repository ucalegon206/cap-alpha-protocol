import React from 'react';

const Footer = () => {
    return (
        <footer className="w-full py-8 px-4 border-t border-slate-800 bg-slate-950 mt-auto">
            <div className="max-w-7xl mx-auto flex flex-col items-center justify-center space-y-4">
                <p className="text-slate-400 text-sm text-center max-w-2xl leading-relaxed">
                    <span className="font-bold text-slate-300">NOTICE:</span> For simulation and entertainment purposes only.
                    The data and predictions provided by the Cap Alpha Protocol are probabilistic simulations and should not be
                    construed as financial, legal, or professional sports management advice.
                </p>
                <div className="flex items-center space-x-2 text-slate-500 text-xs">
                    <span>&copy; {new Date().getFullYear()} Andrew Smith</span>
                    <span className="h-1 w-1 bg-slate-700 rounded-full"></span>
                    <span>All Rights Reserved</span>
                </div>
            </div>
        </footer>
    );
};

export default Footer;
