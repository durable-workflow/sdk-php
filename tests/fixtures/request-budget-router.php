<?php

declare(strict_types=1);

$body = json_decode(file_get_contents('php://input'), true, 512, JSON_THROW_ON_ERROR);
file_put_contents(getenv('DW_BUDGET_RECEIPTS'), json_encode(['worker_id' => $body['worker_id'],
    'path' => $_SERVER['REQUEST_URI']], JSON_THROW_ON_ERROR).PHP_EOL, FILE_APPEND);
usleep(300000);
header('Content-Type: application/json');
echo '{"acknowledged":true}';
