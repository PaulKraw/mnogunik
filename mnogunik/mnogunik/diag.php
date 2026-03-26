<?php
// if (($_GET['key'] ?? '') !== 'super123Lisa') { http_response_code(403); exit('Access denied'); }
header('Content-Type: text/plain; charset=utf-8');

echo "whoami: "; system('whoami'); echo "\n";
echo "pwd: "; system('pwd'); echo "\n";
echo "id: "; system('id'); echo "\n";

echo "\nwhich python3: "; system('which python3'); echo "\n";
echo "python3 -V: "; system('/usr/bin/python3 -V 2>&1'); echo "\n";

echo "\nphp disable_functions: ";
$di = ini_get('disable_functions'); var_dump($di);

echo "\nopen_basedir: "; var_dump(ini_get('open_basedir'));

echo "\nlist /var/www/mnogunik/: \n"; system('ls -la /var/www/mnogunik/ | head -n 20'); echo "\n";
echo "\nlist /var/www/mnogunik/proj: \n"; system('ls -la /var/www/mnogunik/proj | head -n 20'); echo "\n";
echo "list /var/www/mnogunik/outfile: \n"; system('ls -la /var/www/mnogunik/outfile | head -n 20'); echo "\n";

echo "\ntry run go.py (dry-run -V): \n"; system('/usr/bin/python3 -V 2>&1'); echo "\n";

echo "\nweb user can write?\n";
clearstatcache();
printf("outfile writable: %s\n", is_writable('/var/www/mnogunik/outfile') ? 'YES' : 'NO');
printf("mnogunik writable: %s\n", is_writable(__DIR__) ? 'YES' : 'NO');

echo "\nNginx error log tail:\n";
@system('tail -n 50 /var/log/nginx/mnogunik.error.log 2>&1');
