// Synthetic HTTP only. This checks the load driver's drain, not workflow execution.
import http from 'node:http';

const failLastContext = process.env.FAIL_LAST_CONTEXT === 'true';
const contextDelay = Number(process.env.CONTEXT_DELAY_MS || 0);
const originals = new Map();
const observed = new Set();
let earlyActions = 0;
let activityQueries = 0;
let contexts = 0;
let lateAdmissions = 0;
let firstAdmission;

http.createServer(async (request, response) => {
    if (request.url === '/test-state') {
        response.setHeader('Content-Type', 'application/json');
        response.end(JSON.stringify({contexts, admitted: originals.size, observed: observed.size, lateAdmissions, earlyActions, activityQueries}));
        return;
    }

    const user = request.headers.authorization?.replace('Bearer example-', '');
    response.setHeader('X-Request-Id', request.headers['x-request-id'] || '');
    response.setHeader('X-Example-User', user || '');
    response.setHeader('X-Example-Namespace', 'drain-test');
    response.setHeader('X-Worker-Pid', '1');
    response.setHeader('Content-Type', 'application/json');
    if (!['alpha', 'beta'].includes(user)) {
        response.writeHead(401).end('{}');
        return;
    }

    if (request.method === 'GET' && request.url === '/api/context') {
        contexts++;
        if (contextDelay > 0) await new Promise(resolve => setTimeout(resolve, contextDelay));
        if (failLastContext && contexts === 30) {
            response.writeHead(503).end('{"error":"constructed_last_context_failure"}');
            return;
        }
        response.end(JSON.stringify({user, namespace: 'drain-test', client_resolved_at_boot: false}));
        return;
    }
    if (request.method === 'POST' && request.url === '/api/workflows') {
        let body = '';
        for await (const chunk of request) body += chunk;
        const {id, kind} = JSON.parse(body);
        if (!['short', 'timer', 'signal', 'cancel'].includes(kind) || originals.has(id)) {
            response.writeHead(409).end('{}');
            return;
        }
        const now = Date.now();
        firstAdmission ??= now;
        if (now - firstAdmission >= 5000) lateAdmissions++;
        originals.set(id, {started: now, kind, queries: 0, terminal: false});
        response.writeHead(201).end(JSON.stringify({workflow_id: `${user}-${id}`, run_id: id}));
        return;
    }
    const parts = request.url?.replace('/api/workflows/', '').split('/') || [];
    const id = parts[0];
    const original = originals.get(id);
    if (request.method === 'POST' && original && parts[1] === 'query') {
        original.queries++;
        activityQueries++;
        response.end(JSON.stringify({phase: original.queries === 1 ? 'activity' : 'waiting'}));
        return;
    }
    if (request.method === 'POST' && original && ['signal', 'cancel'].includes(parts[1])) {
        if (original.queries < 2 || parts[1] !== original.kind) {
            earlyActions++;
            response.writeHead(409).end('{}');
            return;
        }
        original.terminal = true;
        response.writeHead(204).end();
        return;
    }
    if (request.method === 'GET' && originals.has(id)) {
        if (['signal', 'cancel'].includes(original.kind) ? !original.terminal : Date.now() - original.started < 4000) {
            response.end('{"status":"waiting"}');
            return;
        }
        observed.add(id);
        if (original.kind === 'cancel') {
            response.end('{"status":"cancelled"}');
            return;
        }
        response.end(`{"status":"completed","output":{"user":"${user}","echo":"hello ${user}","number":9007199254740993,"nullable":null,"text":"雪"}}`);
        return;
    }
    response.writeHead(404).end('{}');
}).listen(8080, '0.0.0.0');
