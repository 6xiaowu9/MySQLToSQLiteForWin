@echo off
del /Q .\cache\*
php .\program\convert_data.php
del /Q .\cache\*
Pause