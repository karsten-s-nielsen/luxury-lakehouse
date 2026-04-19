/*
 * Post-install helper: resolves the Taipy-GUI install directory by running
 * `pip show taipy-gui` against the active venv, then writes the result to
 * `.env` as `TAIPY_GUI_DIR=<site-packages-root>` so webpack can locate
 * `taipy-gui-deps-manifest.json` at build time.
 *
 * Adapted from Avaiga's `guiext-template` install.js. One deviation:
 *   - Writes `TAIPY_GUI_DIR` (not `TAIPY_DIR`) so the name matches
 *     `webpack.config.js`. The upstream template has a variable-name
 *     mismatch.
 *
 * Also installs the local `taipy-gui` SDK package into `node_modules/` with
 * `--no-save` so absolute, machine-specific paths don't leak into
 * `package.json`. The types are needed at compile time so TypeScript can
 * resolve the `"taipy-gui"` import; the runtime copy is still provided by
 * Taipy's webapp via the `externals` alias.
 */
require("dotenv").config();
const { execSync } = require("child_process");
const { existsSync, writeFileSync, appendFileSync, readFileSync } = require("fs");
const { sep } = require("path");

function locatePackage(pkg) {
  let out = null;
  try {
    out = execSync(`pip show ${pkg}`, { stdio: ["ignore", "pipe", "ignore"] });
  } catch {
    return null;
  }
  if (!out) return null;
  let location = null;
  let editable = null;
  for (const line of out.toString().split("\n")) {
    if (line.startsWith("Location: ")) location = line.substring(10).trim();
    else if (line.startsWith("Editable project location: ")) editable = line.substring(27).trim();
  }
  return editable || location;
}

function fetchTaipyDir() {
  return process.env.TAIPY_GUI_DIR || locatePackage("taipy-gui") || locatePackage("taipy");
}

function writeEnv(key, value) {
  if (existsSync(".env")) {
    const existing = readFileSync(".env", "utf8");
    if (existing.includes(`${key}=`)) return;
    appendFileSync(".env", `\n${key}=${value}\n`);
  } else {
    writeFileSync(".env", `${key}=${value}\n`);
  }
}

const taipyDir = fetchTaipyDir();
if (!taipyDir || !existsSync(taipyDir)) {
  console.error(
    `Cannot locate a Taipy-GUI installation (TAIPY_GUI_DIR=${taipyDir}).\n` +
      `Activate the venv that has taipy-gui installed, or set TAIPY_GUI_DIR explicitly, then re-run 'npm install'.`
  );
  process.exit(1);
}

const webapp = `${taipyDir}${sep}taipy${sep}gui${sep}webapp`;
if (!existsSync(webapp)) {
  console.error(`Taipy-GUI webapp directory not found at ${webapp}.`);
  process.exit(1);
}

writeEnv("TAIPY_GUI_DIR", taipyDir);
console.log(`TAIPY_GUI_DIR=${taipyDir}`);
console.log(`Taipy-GUI webapp resolved to ${webapp}.`);

// Install the taipy-gui SDK from the local Taipy venv. --no-save keeps the
// absolute path out of package.json (machine-specific, not commit-safe).
// Runtime resolution is still provided via webpack externals.
try {
  execSync(`npm i --no-save "${webapp}"`, { stdio: "inherit" });
  console.log("Local taipy-gui SDK installed into node_modules/.");
} catch (err) {
  console.error("Failed to install local taipy-gui SDK for type resolution:", err.message);
  process.exit(1);
}
