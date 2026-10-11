import { execFileSync, spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

export interface TestServer {
	/** Base URL the server is listening on, e.g. `http://127.0.0.1:53421`. */
	url: string;
	/** Kills the process. Safe to call more than once. */
	stop(): void;
}

/**
 * Builds `cmd/testserver` into a temp dir, spawns it on an ephemeral
 * port and resolves once it prints its address. Owning the build here keeps the
 * test tiers self-contained: no compiled artifact in the tree and no separate
 * `task` build step to remember.
 *
 * Shared by the vitest integration tier and the Playwright e2e tier, which
 * spawn their own instances with different flags.
 */
export async function startTestServer(args: string[] = []): Promise<TestServer> {
	const repoRoot = fileURLToPath(new URL('../../..', import.meta.url));
	const dir = mkdtempSync(join(tmpdir(), 'synctest-'));
	const bin = join(dir, 'testserver');
	// Every run builds a fresh binary of about 40 MB. The running process
	// keeps its executable after the unlink, so the folder goes as soon as
	// the server listens, or when the build or the start fails, and an
	// aborted run leaves nothing behind.
	const removeBuild = () => rmSync(dir, { recursive: true, force: true });
	try {
		execFileSync('go', [ 'build', '-o', bin, './cmd/testserver' ], {
			cwd: repoRoot,
			stdio: 'inherit',
		});
	} catch (err) {
		removeBuild();
		throw err;
	}

	const proc = spawn(bin, args, { stdio: [ 'ignore', 'pipe', 'inherit' ] });

	const url = await new Promise<string>((resolve, reject) => {
		let buf = '';
		proc.stdout.on('data', (chunk: Buffer) => {
			buf += chunk.toString();
			const match = buf.match(/^LISTENING (\S+)$/m);
			if (match?.[1]) resolve(match[1]);
		});
		proc.on('error', reject);
		proc.on(
			'exit',
			(code) => reject(new Error(`testserver exited with ${code} before listening`)),
		);
	}).finally(removeBuild);

	return {
		url,
		stop: () => {
			proc.kill();
		},
	};
}
