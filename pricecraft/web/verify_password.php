<?php
// web/verify_password.php
// Returns true if password ok, false otherwise
function verify_password($password) {
    $hashFile = __DIR__ . '/../config/password_hash.txt';
    if (!file_exists($hashFile)) return false;
    $hash = trim(file_get_contents($hashFile));
    return password_verify($password, $hash);
}
