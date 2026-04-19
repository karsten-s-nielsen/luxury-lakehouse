/*
 * Webpack config for the `ll_ext` Taipy GUI extension.
 *
 * Output: a single UMD bundle `dist/library.js` exposed as the global
 * `LlExt` (matches the JS module name derived from the Python library
 * name `ll_ext` via Taipy's default `_to_camel_case`). The Taipy Flask
 * server serves this file at `/taipy-extension/ll_ext/front-end/dist/library.js`.
 *
 * Runtime dependencies (React, @mui/material, @emotion, taipy-gui) are
 * kept OUT of this bundle and resolved at runtime from Taipy's shared DLL
 * (`taipy-gui-deps.dll.js`) via webpack's DllReferencePlugin.
 */
const webpack = require("webpack");
const path = require("path");
require("dotenv").config();

module.exports = (_env, options) => ({
  mode: options.mode,
  entry: ["./src/index.ts"],
  output: {
    filename: "library.js",
    path: path.resolve(__dirname, "dist"),
    library: {
      name: "LlExt",
      type: "umd",
    },
    publicPath: "/",
  },
  // The `taipy-gui` SDK is provided globally by Taipy's webapp at runtime.
  externals: {
    "taipy-gui": "TaipyGui",
  },
  devtool: options.mode === "development" && "inline-source-map",
  resolve: {
    extensions: [".webpack.js", ".web.js", ".ts", ".tsx", ".js"],
  },
  module: {
    rules: [
      {
        test: /\.tsx?$/,
        use: "ts-loader",
        exclude: /node_modules/,
      },
    ],
  },
  plugins: [
    new webpack.DllReferencePlugin({
      manifest: path.resolve(
        __dirname,
        `${process.env.TAIPY_GUI_DIR}/taipy/gui/webapp/taipy-gui-deps-manifest.json`
      ),
      name: "TaipyGuiDependencies",
    }),
  ],
});
