import globals from "globals";
import pluginJs from "@eslint/js";
import tseslint from "typescript-eslint";

/** @type {import('eslint').Linter.Config[]} */
export default [
  {
    files: ["**/*.{js,cjs,mjs,ts}"],
    languageOptions: { globals: globals.node },
  },
  {
    ignores: [
      "**/dist/**",
      "**/node_modules/**",
      "**/*.config.mjs",
      "**/*.json",
    ],
  },
  pluginJs.configs.recommended,
  ...tseslint.configs.recommended,
];
