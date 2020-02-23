:a
SETLOCAL
set timevar=access.data%timevar%%time:~3,2%%time:~6,2%.log
chcp 65001
python C:\Users\Jiaot\Desktop\CloudCompiutering\Log.py > %timevar%
start putWin10.cmd %timevar%
echo succeed
timeout /T 10 /NOBREAK
ENDLOCAL
goto a