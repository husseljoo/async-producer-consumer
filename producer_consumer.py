import asyncio, asyncssh, sys

localmachine_con = None
sftp_con = None


async def establish_connections():
    global localmachine_con, sftp_con
    host1, port1, username1, password1 = "localhost", 22, "husseljo", "husseljo"
    host2, port2, username2, password2 = "192.168.56.107", 22, "root", "husseljo"
    localmachine_con = await asyncssh.connect(
        host1, port1, username=username1, password=password1
    )
    sftp_con = await asyncssh.connect(
        host2, port2, username=username2, password=password2
    )


async def producer(queue, name):
    for i in range(5):
        await asyncio.sleep(1)  # Simulate some asynchronous task
        item = f"{name}-{i}"
        await queue.put(item)
        print(f"{name} produced {item}")


async def consumer(queue, name):
    while True:
        item = await queue.get()
        print(f"item: {item}")
        await asyncio.sleep(2)  # Simulate some processing time
        print(f"{name} consumed {item}")
        queue.task_done()


async def main():
    await establish_connections()
    queue = asyncio.Queue()
    result = await localmachine_con.run("ls /home/husseljo/damn/nba/")
    print("Command output:", result.stdout)
    print("Exit_status output:", result.exit_status)

    # Create producers
    producers = [producer(queue, f"Producer-{i}") for i in range(2)]

    # Create consumers
    consumers = [consumer(queue, f"Consumer-{i}") for i in range(3)]

    # Start producers and consumers
    await asyncio.gather(*producers, *consumers)

    # Wait for the queue to be empty
    await queue.join()


asyncio.run(main())
