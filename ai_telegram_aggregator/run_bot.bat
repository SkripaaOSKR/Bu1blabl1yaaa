@echo off
chcp 65001 > nul
title AI News Aggregator Control Panel
cls
color 0B

:menu
echo ======================================================
echo           AI NEWS AGGREGATOR - CONTROL PANEL
echo ======================================================
echo.
echo  1) START (Запуск)
echo  2) FAST UPDATE (Быстрое обновление кода - 5 сек)
echo  3) FULL REBUILD (Полная пересборка с нуля - долго)
echo  4) STOP (Остановить всё)
echo  5) LOGS BOT (Логи Telegram-бота)
echo  6) LOGS WORKER (Логи парсера и ИИ)
echo  7) LOGS API (Логи сервера и админки)
echo  8) RESTART (Перезагрузка)
echo  9) CLEAN (Очистка мусора)
echo 10) BACKUP DB (Умный бэкап с датой)
echo 11) EXIT
echo.
echo ======================================================
set /p choice="Выберите действие (1-11): "

if "%choice%"=="1" goto start
if "%choice%"=="2" goto fast_update
if "%choice%"=="3" goto full_rebuild
if "%choice%"=="4" goto stop
if "%choice%"=="5" goto logs_bot
if "%choice%"=="6" goto logs_worker
if "%choice%"=="7" goto logs_api
if "%choice%"=="8" goto restart
if "%choice%"=="9" goto clean
if "%choice%"=="10" goto backup
if "%choice%"=="11" goto exit

:start
echo [+] Запускаю контейнеры...
docker-compose up -d
pause
goto menu

:fast_update
echo [+] Быстрое обновление (только измененные файлы Python)...
docker-compose up -d --build
echo [!] Обновление завершено.
pause
goto menu

:full_rebuild
echo [+] Останавливаю контейнеры...
docker-compose down
echo [+] Полная пересборка без кэша (будут заново скачаны все библиотеки)...
docker-compose build --no-cache
echo [+] Запускаю чистую версию...
docker-compose up -d --force-recreate
echo [!] Полная пересборка завершена.
pause
goto menu

:stop
echo [-] Останавливаю сервисы...
docker-compose down
pause
goto menu

:logs_bot
echo [*] Логи бота (Ctrl+C для выхода в меню)...
docker-compose logs -f bot
goto menu

:logs_worker
echo [*] Логи воркера (Ctrl+C для выхода в меню)...
docker-compose logs -f worker
goto menu

:logs_api
echo [*] Логи API сервера (Ctrl+C для выхода в меню)...
docker-compose logs -f api
goto menu

:restart
echo [!] Перезагружаю...
docker-compose restart
pause
goto menu

:clean
echo [!] Удаляю мусорные образы (dangling images)...
docker system prune -f
pause
goto menu

:backup
:: Получаем надежную дату и время от системы Windows
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set datetime=%%I
set backup_name=backup_%datetime:~0,4%-%datetime:~4,2%-%datetime:~6,2%_%datetime:~8,2%-%datetime:~10,2%.sql

echo [*] Создаю бэкап базы данных...
docker exec ai_news_db pg_dump -U news news > %backup_name%
echo [!] Бэкап успешно сохранен в файл: %backup_name%
pause
goto menu

:exit
exit