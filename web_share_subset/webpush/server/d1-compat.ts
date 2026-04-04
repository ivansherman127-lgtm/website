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
  batch: (stmts: BoundStatement[]) => Promise<{ results?: unknown[] }[]>;
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
      _query: query,
      _args: args,
    } as BoundStatement & { _query: string; _args: unknown[] };
  }

  return {
    prepare(query: string): BoundStatement {
      return makeBound(query, []);
    },
    async batch(stmts: BoundStatement[]): Promise<{ results?: unknown[] }[]> {
      // Run all statements inside a single transaction for atomicity and performance.
      const runAll = db.transaction(() => {
        const out: { results?: unknown[] }[] = [];
        for (const stmt of stmts) {
          const s = stmt as unknown as { _query: string; _args: unknown[] };
          const prepared = db.prepare(s._query);
          const result = prepared.run(...(s._args ?? []));
          out.push({ results: [result] });
        }
        return out;
      });
      return runAll();
    },
  };
}
