import asyncio
import signal


def register_stop_event():
    """
    Регистрирует события для обеспечения Graceful Shutdown
    """

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    # Функция-хэндлер для сигнала
    def shutdown_handler():
        print("\n[Shutdown] Получен сигнал остановки! Завершаем работу...")
        stop_event.set()  # Взводим флаг остановки

    # Регистрируем хэндлеры для Ctrl+C (SIGINT) и сигналов завершения Docker/K8s (SIGTERM)
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_handler)

    return stop_event
