# CatDb Docs

This folder contains the Docusaurus documentation site for CatDb.

## Install

Dependencies are managed with npm and locked in `package-lock.json`.

```bash
npm install
```

## Local development

```bash
npm start
```

## Build

```bash
npm run build
```

The production output is generated in `build/`.

The site reads the CatDb release version from `../Directory.Build.props`.
When you bump `<VersionPrefix>`, the navbar, homepage, and package install
snippet update on the next Docusaurus build/start.

## Type check

```bash
npm run typecheck
```
