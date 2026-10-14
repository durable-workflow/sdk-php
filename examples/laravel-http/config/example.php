<?php

return [
    'eager' => (bool) env('EXAMPLE_EAGER', false),
    // These are disposable application identities, not runtime credentials.
    'users' => ['example-alpha' => 'alpha', 'example-beta' => 'beta'],
];
