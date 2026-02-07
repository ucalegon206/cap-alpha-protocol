/** @type {import('next').NextConfig} */
const nextConfig = {
    experimental: {
        serverComponentsExternalPackages: ['duckdb', '@mapbox/node-pre-gyp', 'node-gyp'],
    },
    webpack: (config, { isServer }) => {
        if (isServer) {
            config.externals.push('duckdb');
        }
        return config;
    },
};

module.exports = nextConfig;
