import type { DaytonaSandbox } from "@alchemy.run/daytona/SandboxClient";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";

/** Files: folders, uploads, search, replace, download, move, permissions, delete. */
export const files = Effect.fn(function* (box: DaytonaSandbox, prefix: string) {
  const dir = `/tmp/alchemy-fs-${prefix}`;
  yield* box.fs.deleteFile({ path: dir, recursive: true }).pipe(Effect.ignore);
  yield* box.fs.createFolder({ path: dir, mode: "755" });
  yield* box.fs.uploadFile({ path: `${dir}/a.txt`, body: "hello world\n" });
  yield* box.fs.uploadFile({ path: `${dir}/b.bin`, body: new Uint8Array([0, 1, 2, 255]) });
  const listed = yield* box.fs.listFiles({ path: dir });
  const found = yield* box.fs.findInFiles({ path: dir, pattern: "hello" });
  const searched = yield* box.fs.searchFiles({ path: dir, pattern: "*.txt" });
  yield* box.fs.replaceInFiles({ files: [`${dir}/a.txt`], pattern: "world", newValue: "daytona" });
  const text = new TextDecoder().decode(yield* box.fs.downloadFile({ path: `${dir}/a.txt` }));
  const bytes = Array.from(yield* box.fs.downloadFile({ path: `${dir}/b.bin` }));
  yield* box.fs.moveFile({ source: `${dir}/a.txt`, destination: `${dir}/c.txt` });
  yield* box.fs.setFilePermissions({ path: `${dir}/c.txt`, mode: "600" });
  const moved = yield* box.fs.getFileInfo({ path: `${dir}/c.txt` });
  yield* box.fs.deleteFile({ path: dir, recursive: true });
  const gone = yield* box.exec(`test -e ${dir} && echo present || echo gone`);
  return {
    listed: listed.map((f) => f.name).sort(),
    found: found.map((m) => ({ file: m.file.split("/").pop(), line: m.line })),
    searched: searched.files.map((f) => f.split("/").pop()),
    text,
    bytes,
    moved: { name: moved.name, permissions: moved.permissions },
    gone: gone.result.trim(),
  };
});

/** Processes: a stateful session, an async command and its logs, code runs, PTYs. */
export const processes = Effect.fn(function* (box: DaytonaSandbox, prefix: string) {
  const sessionId = `alchemy-${prefix}`;
  yield* box.process.deleteSession({ sessionId }).pipe(Effect.ignore);
  yield* box.process.createSession({ sessionId });
  yield* box.process.sessionExecuteCommand({ sessionId, command: "export GREETING=hi" });
  const echoed = yield* box.process.sessionExecuteCommand({
    sessionId,
    command: "echo $GREETING",
  });
  const started = yield* box.process.sessionExecuteCommand({
    sessionId,
    command: "sleep 1 && echo done",
    runAsync: true,
  });
  const logs = yield* box.process
    .getSessionCommandLogs({ sessionId, commandId: started.cmdId })
    .pipe(
      Effect.repeat({
        schedule: Schedule.spaced("1 second"),
        until: (l) => l.includes("done"),
        times: 15,
      }),
    );
  const listed = yield* box.process.listSessions();
  const session = yield* box.process.getSession({ sessionId });
  yield* box.process.deleteSession({ sessionId });

  const code = yield* box.process.runCode({ language: "python", code: "print(6 * 7)" });

  const ptyId = `alchemy-pty-${prefix}`;
  yield* box.process.deletePtySession({ sessionId: ptyId }).pipe(Effect.ignore);
  yield* box.process.createPtySession({ id: ptyId, cols: 80, rows: 24 });
  const ptys = yield* box.process.listPtySessions();
  yield* box.process.resizePtySession({ sessionId: ptyId, cols: 120, rows: 40 });
  const resized = yield* box.process.getPtySession({ sessionId: ptyId });
  yield* box.process.deletePtySession({ sessionId: ptyId });

  return {
    echoed: (echoed.output ?? echoed.stdout ?? "").trim(),
    asyncLogs: logs.trim(),
    listed: listed.some((s) => s.sessionId === sessionId),
    commands: session.commands?.length ?? 0,
    code: (code.result ?? "").trim(),
    pty: ptys.sessions.some((p) => p.id === ptyId),
    resized: { cols: resized.cols, rows: resized.rows },
  };
});

/** Git: init, commit, branch, history, status, and a clone. */
export const git = Effect.fn(function* (box: DaytonaSandbox, prefix: string) {
  const path = `/tmp/alchemy-git-${prefix}`;
  yield* box.exec(`rm -rf ${path} ${path}-clone`);
  yield* box.git.initRepository({ path, initial_branch: "main" });
  // Before the first commit HEAD names nothing, so status has no branch.
  const empty = yield* box.git.getStatus({ path }).pipe(Effect.flip);
  yield* box.fs.uploadFile({ path: `${path}/README.md`, body: "# alchemy\n" });
  yield* box.git.addFiles({ path, files: ["README.md"] });
  const commit = yield* box.git.commitChanges({
    path,
    message: "first",
    author: "Alchemy",
    email: "test@alchemy.run",
  });
  const status = yield* box.git.getStatus({ path });
  yield* box.git.createBranch({ path, name: "feature" });
  yield* box.git.checkoutBranch({ path, branch: "feature" });
  const branches = yield* box.git.listBranches({ path });
  const history = yield* box.git.getCommitHistory({ path });
  yield* box.git.cloneRepository({
    url: "https://github.com/octocat/Hello-World.git",
    path: `${path}-clone`,
    depth: 1,
  });
  const cloned = yield* box.fs.listFiles({ path: `${path}-clone` });
  yield* box.exec(`rm -rf ${path} ${path}-clone`);
  return {
    empty: empty._tag,
    status: { branch: status.currentBranch, clean: status.fileStatus.length === 0 },
    hash: commit.hash.length,
    branches: [...branches.branches].sort(),
    current: branches.current,
    history: history.map((c) => c.message.trim()),
    cloned: cloned.some((f) => f.name === "README"),
  };
});

/** Language servers: open a TypeScript file, list its symbols, complete. */
export const lsp = Effect.fn(function* (box: DaytonaSandbox, prefix: string) {
  const pathToProject = `/tmp/alchemy-lsp-${prefix}`;
  const uri = `file://${pathToProject}/index.ts`;
  yield* box.exec(`rm -rf ${pathToProject} && mkdir -p ${pathToProject}`);
  yield* box.fs.uploadFile({
    path: `${pathToProject}/index.ts`,
    body: "export function greet(name: string) {\n  return name.toUpperCase();\n}\n",
  });
  const languageId = "typescript";
  yield* box.lsp.start({ languageId, pathToProject });
  yield* box.lsp.didOpen({ languageId, pathToProject, uri });
  const symbols = yield* box.lsp.documentSymbols({ languageId, pathToProject, uri });
  const completions = yield* box.lsp.completions({
    languageId,
    pathToProject,
    uri,
    position: { line: 1, character: 14 },
  });
  yield* box.lsp.stop({ languageId, pathToProject });
  return {
    symbols: symbols.map((s) => s.name),
    completes: completions.items.some((i) => i.label === "toUpperCase"),
  };
});

/** Interpreter contexts, ports, versions, home directory and system metrics. */
export const system = Effect.fn(function* (box: DaytonaSandbox, prefix: string) {
  const context = yield* box.interpreter.createInterpreterContext({ language: "python" });
  const contexts = yield* box.interpreter.listInterpreterContexts();
  yield* box.interpreter.deleteInterpreterContext({ id: context.id });

  const sessionId = `alchemy-http-${prefix}`;
  yield* box.process.deleteSession({ sessionId }).pipe(Effect.ignore);
  yield* box.process.createSession({ sessionId });
  yield* box.process.sessionExecuteCommand({
    sessionId,
    command: "python3 -m http.server 8123",
    runAsync: true,
  });
  const inUse = yield* box.ports.isPortInUse({ port: 8123 }).pipe(
    Effect.repeat({
      schedule: Schedule.spaced("1 second"),
      until: (r) => r.isInUse === true,
      times: 15,
    }),
  );
  const ports = yield* box.ports.getPorts();
  yield* box.process.deleteSession({ sessionId });

  const home = yield* box.info.getUserHomeDir();
  const version = yield* box.info.getVersion();
  const metrics = yield* box.system.getSystemMetrics();
  return {
    context: context.language,
    listed: contexts.contexts.some((c) => c.id === context.id),
    inUse: inUse.isInUse,
    ports: (ports.ports ?? []).includes(8123),
    home: home.dir,
    version: Object.keys(version).length > 0,
    cpus: metrics.cpuCount,
    memory: (metrics.memTotal ?? 0) > 0,
  };
});
