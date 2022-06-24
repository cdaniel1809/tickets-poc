for (($i = 0), ($j = 0); $i -lt 128; $i++)
{
	invoke-expression 'cmd /c start powershell -Command { write-host "Hi, new window!"; set-location "C:\bside-projects\oxxo\poc"; python receiveEventHub.py}'
}