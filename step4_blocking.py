"""Шаг 4 (дополнительный). Блокировка event loop.

Ниже готовый асинхронный сервер из шага 3б. Но теперь клиент "bob"
прислал не запрос в базу, а задачу на тяжёлые вычисления: функция
heavy_compute. Она синхронная и работает 1-3 секунды в зависимости от машины.

Задание 4а:
  В handle_request для клиента "bob" вызовите heavy_compute()
  вместо await asyncio.sleep(duration). Остальные клиенты по-прежнему
  ждут через asyncio.sleep. Запустите.
  Что произошло с остальными клиентами? Почему, ведь всё написано на async?

Задание 4б:
  Исправьте так, чтобы остальные клиенты не ждали bob. Два инструмента:
    await asyncio.to_thread(heavy_compute)
    await loop.run_in_executor(pool, heavy_compute)
  где loop = asyncio.get_running_loop(), а pool — это ProcessPoolExecutor
  из concurrent.futures, открытый через with в main.
  Попробуйте оба. Порядок строк в выводе должен стать как в шаге 3б,
  время bob зависит от машины.

Задание 4в (если осталось время):
  Сделайте тяжёлыми трёх клиентов: bob, dave и eve. Сравните общее время
  для to_thread и для пула процессов. Почему оно отличается? Вспомните GIL.
"""

import asyncio

from timer import log, print_time
from concurrent.futures import ProcessPoolExecutor 

CLIENTS = [
    ("alice", 0.5),
    ("bob", 5.0),
    ("carol", 0.5),
    ("dave", 0.5),
    ("eve", 0.5),
]


def heavy_compute() -> int:
    """Синхронные тяжёлые вычисления, 1-3 секунды."""
    h = 0
    for i in range(30_000_000):
        h = (h * 31 + i) & 0xFFFFFFFF
    return h


async def handle_request(client: str, duration: float) -> None:
    log(f"{client}: запрос принят")
    if client == "bob":
        heavy_compute()
    else:
      await asyncio.sleep(duration)
    log(f"{client}: ответ отправлен")



async def async_handle_request1(client: str, duration: float) -> None:
    log(f"{client}: запрос принят")
    if client == "bob":
        await asyncio.to_thread(heavy_compute)   
    else:
        await asyncio.sleep(duration)
    log(f"{client}: ответ отправлен")


async def async_handle_request2(client: str, duration: float, pool: ProcessPoolExecutor) -> None:
    log(f"{client}: запрос принят")
    if client == "bob":
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(pool, heavy_compute)   
    else:
        await asyncio.sleep(duration)
    log(f"{client}: ответ отправлен")

HEAVY = {"bob", "dave", "eve"}

async def async_handle_request_for_many(client, duration, pool=None):
    log(f"{client}: запрос принят")
    if client in HEAVY:
        if pool is None:
            await asyncio.to_thread(heavy_compute)
        else:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(pool, heavy_compute)
    else:
        await asyncio.sleep(duration)
    log(f"{client}: ответ отправлен")



async def main_a() -> None:
    coroutines = [handle_request(client, duration) for client, duration in CLIENTS]
    await asyncio.gather(*coroutines)


async def main_b1() -> None:
    coroutines = [
        async_handle_request1(client, duration)
        for client, duration in CLIENTS
    ]
    await asyncio.gather(*coroutines)


async def main_b2() -> None:
    with ProcessPoolExecutor() as pool:
        coroutines = [
            async_handle_request2(client, duration, pool)
            for client, duration in CLIENTS
        ]
        await asyncio.gather(*coroutines)


async def main_c_thread() -> None:
    coroutines = [
        async_handle_request_for_many(client, duration)
        for client, duration in CLIENTS
    ]
    await asyncio.gather(*coroutines)


async def main_c_pool() -> None:
    with ProcessPoolExecutor() as pool:
        coroutines = [
            async_handle_request_for_many(client, duration, pool)
            for client, duration in CLIENTS
        ]
        await asyncio.gather(*coroutines)



if __name__ == "__main__":
    print("=== 4а: bob блокирует event loop ===")
    with print_time():
        asyncio.run(main_a())

    print("=== 4б-1: to_thread ===")
    with print_time():
        asyncio.run(main_b1())

    print("=== 4б-2: ProcessPoolExecutor ===")
    with print_time():
        asyncio.run(main_b2())

    print("=== 4в-1: три тяжёлых через to_thread ===")
    with print_time():
        asyncio.run(main_c_thread())

    print("=== 4в-2: три тяжёлых через пул процессов ===")
    with print_time():
        asyncio.run(main_c_pool())