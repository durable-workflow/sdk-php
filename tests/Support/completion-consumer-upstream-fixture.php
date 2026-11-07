<?php

declare(strict_types=1);

// Fixed HTTP data for relay source controls. No workflow state, Server implementation or runtime claim.
$raw = file_get_contents('php://input');
$headers = array_change_key_case(getallheaders(), CASE_LOWER);
header('Content-Type: application/json');
if (($headers['authorization'] ?? null) !== 'Bearer synthetic-relay-role') {
    http_response_code(403);
    echo '{"reason":"fixture_role_not_forwarded"}';
    return;
}
$body = json_decode($raw, true, flags: JSON_THROW_ON_ERROR);
if (($body['fixture_failure'] ?? false) === true) {
    http_response_code(500);
    echo '{"reason":"fixture_original_upstream_failure"}';
    return;
}
echo json_encode(['task' => null, 'poll_status' => 'empty', 'fixture_wire_sha256' => hash('sha256', $raw)], JSON_THROW_ON_ERROR);
