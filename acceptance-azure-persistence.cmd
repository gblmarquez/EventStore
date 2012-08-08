@ECHO OFF
SETLOCAL

set MSPEC_RUNNER_X86="src/packages/Machine.Specifications-Signed.0.5.3.0/tools/mspec-x86-clr4.exe"

ECHO === Building ===
"C:/WINDOWS/Microsoft.NET/Framework/v4.0.30319/msbuild.exe" /nologo /verbosity:quiet src/EventStore.sln /p:Configuration=Debug /p:Platform="Any CPU"

rem ECHO === In Memory ===
rem CALL :run_test InMemoryPersistence

ECHO === Cloud Storage ===
CALL :run_test AzureBlobPersistence

ENDLOCAL
GOTO :eof 

:run_test <persistence> <host> <port> <database> <user> <password>
SETLOCAL

SET persistence=%~1
SET host=%~2
SET port=%~3
SET database=%~4
SET user=%~5
SET password=%~6

ECHO ===============
ECHO TESTING: %~1
%MSPEC_RUNNER_X86% "src/tests/EventStore.Persistence.AcceptanceTests/bin/Debug/EventStore.Persistence.AcceptanceTests.dll"
ECHO.

ENDLOCAL
