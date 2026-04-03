/**
 * Thin D1-compatible adapter over better-sqlite3.
 *
 * Implements exactly the interface used by functions/api/utm.ts so the same
 * handler code runs unchanged on the Node.js server.
 */

import Database from "better-sqlite3";

type BoundStatement = {
  bind: (...args: unknown[]) => BoundStatement;
  all: <T = Record<string, unknown>>() => Promise<{ results?: T[] }>;
  first: <T = Record<string, unknown>>() => Promise<T | null>;
  run: () => Promise<unknown>;
};

export type D1Compat = {
  prepare: (query: string) => BoundStatement;
};

export function createD1Compat(db: Database.Database): D1Compat {
  function makeBound(query: string, args: unknown[]): BoundStatement {
    return {
      bind(...newArgs: unknown[]): BoundStatement {
        return makeBound(query, newArgs);
      },
      async all<T = Record<string, unknown>>(): Promise<{ results?: T[] }> {
        const results = db.prepare(query).all(...args) as T[];
        return { results };
      },
      async first<T = Record<string, unknown>>(): Promise<T | null> {
        const row = db.prepare(query).get(...args) as T | undefined;
        return row ?? null;
      },
      async run(): Promise<unknown> {
        return db.prepare(query).run(...args);
      },
    };
  }

  return {
    prepare(query: string): BoundStatement {
      return makeBound(query, []);
    },
  };
}
