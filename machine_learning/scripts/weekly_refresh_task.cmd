@echo off
rem The weekly production refresh as Task Scheduler launches it (task ff-weekly-refresh on Tuesdays, or a one-shot task).
rem Runs weekly_refresh_task.sh under Git Bash; its lines append to machine_learning\_cache\logs\weekly_refresh_task.log,
rem the run's own log sits beside it (production_<stamp>.log). Double-click to run by hand.
set "D=%~dp0"
set "D=%D:\=/%"
if not exist "%~dp0..\_cache\logs" mkdir "%~dp0..\_cache\logs"
"C:\Program Files\Git\bin\bash.exe" --login -c "bash '%D%weekly_refresh_task.sh' >> '%D%../_cache/logs/weekly_refresh_task.log' 2>&1"
