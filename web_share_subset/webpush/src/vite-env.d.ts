/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_DATA_SOURCE?: "static" | "d1";
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
