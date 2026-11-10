@echo off
rem The weekly production refresh as Task Scheduler launches it (task ff-weekly-refresh, Tuesdays and Wednesdays 06:30
rem with the argument --when-ready, or a one-shot task for a run now, no argument). Runs weekly_refresh_task.sh under
rem Git Bash with the same arguments; its lines append to machine_learning\_cache\logs\weekly_refresh_task.log, the
rem run's own log sits beside it (production_<stamp>.log). Double-click to run by hand, at once.
set "D=%~dp0"
set "D=%D:\=/%"
if not exist "%~dp0..\_cache\logs" mkdir "%~dp0..\_cache\logs"
"C:\Program Files\Git\bin\bash.exe" --login -c "bash '%D%weekly_refresh_task.sh' %* >> '%D%../_cache/logs/weekly_refresh_task.log' 2>&1"
