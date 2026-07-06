/** @type {import('next').NextConfig} */
const nextConfig = {
  // Static export so the whole app can be embedded in the Go binary (design 部署).
  output: "export",
  // No Next image optimization server in a static export.
  images: { unoptimized: true },
  // Emit trailing-slash-free .html files that pair with the Go SPA fallback.
  trailingSlash: false,
};

export default nextConfig;
