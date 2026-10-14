<?php

use App\Http\Controllers\WorkflowController;
use DurableWorkflow\Client;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Route;

Route::middleware('example')->group(function (): void {
    Route::get('context', function (Request $request, Client $client) {
        return response()->json([
            'user' => $request->attributes->get('example_user'),
            'namespace' => $client->namespace,
        ]);
    });
    Route::post('workflows', [WorkflowController::class, 'start']);
    Route::get('workflows/{id}', [WorkflowController::class, 'status']);
    Route::post('workflows/{id}/signal', [WorkflowController::class, 'signal']);
    Route::post('workflows/{id}/query', [WorkflowController::class, 'query']);
    Route::post('workflows/{id}/cancel', [WorkflowController::class, 'cancel']);
});
