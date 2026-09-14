import { build, version } from "esbuild"
import { mkdir, readFile, writeFile } from "node:fs/promises"
import { gzipSync } from "node:zlib"
import { isBuiltin } from "node:module"
import { resolve } from "node:path"
import { pathToFileURL } from "node:url"

const output = resolve(process.env.BENCH_OUTPUT ?? ".benchmark")
await mkdir(output, { recursive: true })
const reports = []
for (const profile of ["bundled", "effect-external"]) {
  for (const adapter of ["native", "kafkajs"]) {
    const outfile = resolve(output, `${adapter}-${profile}.mjs`)
    const result = await build({
      entryPoints: [`benchmarks/entries/${adapter}.mjs`], outfile, bundle: true, minify: true,
      platform: "node", target: "node24", format: "esm", treeShaking: true, sourcemap: false,
      metafile: true, external: profile === "effect-external" ? ["effect", "effect/*"] : [],
      // Resolve workspace imports to the same built core module in both adapters.
      alias: { "@effect-kafka/core": resolve("packages/core/dist/index.js") },
      banner: { js: 'import { createRequire as __createRequire } from "node:module"; const require = __createRequire(import.meta.url);' }
    })
    const inputs = Object.keys(result.metafile.inputs)
    if (adapter === "native" && inputs.some((name) => /(?:kafkajs|confluent|saslprep@|sparse-bitfield|memory-pager)/.test(name))) throw new Error("Native bundle contains an unexpected dependency")
    const imports = [...new Set(Object.values(result.metafile.outputs).flatMap((entry) => entry.imports.filter((i) => i.external).map((i) => i.path)))].sort()
    if (imports.some((name) => !isBuiltin(name) && !(profile === "effect-external" && /^effect(?:\/|$)/.test(name)))) throw new Error("Unexpected external runtime dependency")
    const bytes = await readFile(outfile)
    // The full emitted bundle must execute, not just have an attractive byte count.
    if (profile === "bundled") {
      const emitted = await import(pathToFileURL(outfile).href)
      if (typeof emitted.run !== "function") throw new Error("Missing bundle entrypoint")
      if (adapter === "native") await emitted.run(["localhost:1"], [])
    }
    reports.push({ adapter, profile, rawBytes: bytes.length, gzipBytes: gzipSync(bytes).length, externalImports: imports })
    await writeFile(outfile + ".meta.json", JSON.stringify(result.metafile, null, 2) + "\n")
  }
}
await writeFile(resolve(output, "bundle-sizes.json"), JSON.stringify({ esbuild: version, node: process.version, target: "node24", format: "esm", minified: true, reports }, null, 2) + "\n")
console.table(reports.map(({ externalImports, ...row }) => row))
