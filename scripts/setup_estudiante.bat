@echo off
echo ======================================================================
echo    CONFIGURACION DEL ENTORNO DE SPARK PARA WINDOWS (CURSO 2026)
echo ======================================================================
echo.

:: 1. Crear carpeta C:\hadoop\bin si no existe
if not exist "C:\hadoop\bin" (
    echo [*] Creando directorio C:\hadoop\bin...
    mkdir "C:\hadoop\bin"
)

:: 2. Descargar binarios nativos de Hadoop
if not exist "C:\hadoop\bin\winutils.exe" (
    echo [*] Descargando winutils.exe...
    powershell -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; (New-Object Net.WebClient).DownloadFile('https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe', 'C:\hadoop\bin\winutils.exe')"
)

if not exist "C:\hadoop\bin\hadoop.dll" (
    echo [*] Descargando hadoop.dll...
    powershell -Command "[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; (New-Object Net.WebClient).DownloadFile('https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/hadoop.dll', 'C:\hadoop\bin\hadoop.dll')"
)

:: 3. Copiar hadoop.dll a System32 para deteccion global inmediata por Java
echo [*] Registrando hadoop.dll en System32...
copy /Y "C:\hadoop\bin\hadoop.dll" "%WINDIR%\System32\" >nul 2>&1

:: 4. Variables de entorno permanentes para el usuario
echo [*] Configurando variables de entorno HADOOP_HOME y SPARK_LOCAL_IP...
setx HADOOP_HOME "C:\hadoop" >nul
setx hadoop.home.dir "C:\hadoop" >nul
setx SPARK_LOCAL_IP "127.0.0.1" >nul

echo.
echo ======================================================================
echo    CONFIGURACION COMPLETADA CON EXITO
echo    Cierra y vuelve a abrir tu terminal o VS Code para aplicar cambios.
echo ======================================================================
pause