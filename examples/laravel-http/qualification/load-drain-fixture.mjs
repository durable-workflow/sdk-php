// Synthetic HTTP only. This checks the load driver's drain, not workflow execution.
import http from 'node:http';

const failLastContext = process.env.FAIL_LAST_CONTEXT === 'true';
const originals = new Map();
const observed = new Set();
let contexts = 0;
let lateAdmissions = 0;
let firstAdmission;

http.createServer(async (request, response) => {
    if (request.url === '/test-state') {
        response.setHeader('Content-Type', 'application/json');
        response.end(JSON.stringify({contexts, admitted: originals.size, observed: observed.size, lateAdmissions}));
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
        if (kind !== 'short' || originals.has(id)) {
            response.writeHead(409).end('{}');
            return;
        }
        const now = Date.now();
        firstAdmission ??= now;
        if (now - firstAdmission >= 5000) lateAdmissions++;
        originals.set(id, now);
        response.writeHead(201).end(JSON.stringify({workflow_id: `${user}-${id}`, run_id: id}));
        return;
    }
    const id = request.url?.replace('/api/workflows/', '');
    if (request.method === 'GET' && originals.has(id)) {
        if (Date.now() - originals.get(id) < 4000) {
            response.end('{"status":"waiting"}');
            return;
        }
        observed.add(id);
        response.end(`{"status":"completed","output":{"user":"${user}","echo":"hello ${user}","number":9007199254740993,"nullable":null,"text":"雪"}}`);
        return;
    }
    response.writeHead(404).end('{}');
}).listen(8080, '0.0.0.0');
