/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_DATA_SOURCE?: "d1";
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
