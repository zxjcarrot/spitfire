rem ======================--------===========================
rem                   Make Windows DLL
rem Copyright (C) 2010-2013 Zilong Tan <eric.zltan@gmail.com>
rem =========================================================

@echo off

@cls

@if exist *.o del *.o

@for /R %%a in (*.a) do echo Extracting %%a ... && ar x %%a

@echo Building ULIB.DLL ...

@dllwrap.exe --driver-name x86_64-w64-mingw32-c++ --export-all-symbols
-static -mtune=native -march=native -msse2 -mfpmath=sse -LC:/MinGW/lib -LC:/MinGW64/lib --add-stdcall-alias -mthreads -O3 -o ulib.dll --implib ulib.lib libulib.a -lpthread
@if errorlevel 1 goto dll_error

@del *.o

goto done

:dll_error
@echo "dllwrap failed."
@goto done

:done
@pause
