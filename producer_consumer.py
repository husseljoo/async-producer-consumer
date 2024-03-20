import asyncio, asyncssh, sys
import time

localmachine_con = None
sftp_con = None
items_consumed = 0


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


async def producer_task_copying(queue, record):
    print(f"STARTING to produce {record}\n")
    await asyncio.sleep(1)  # Simulate some asynchronous task
    finished_record = f"{record}-finished"
    await queue.put(finished_record)
    print(f"producer_task_copying produced {finished_record}")


async def producer(queue, name):
    for i in range(5):
        await asyncio.sleep(1)  # Simulate some asynchronous task
        item = f"{name}-{i}"
        await queue.put(item)
        print(f"{name} produced {item}")


async def consumer(queue, id):
    global items_consumed
    print(f"CONSUMER {id} STARTED")
    while True:
        item = await queue.get()
        print(f"item: {item}")
        if item == "STOP":
            # Terminate the consumer when "STOP" is encountered
            break
        # Perform additional processing on the metadata (processed record)
        print(f"Consumer {id} processing metadata of {item}.....")
        await asyncio.sleep(2)  # Simulate some asynchronous task
        metadata = f"Metadata: {item}"
        print(f"Consumer {id} processed metadata: {metadata}")
        items_consumed += 1
        queue.task_done()


async def main():
    await establish_connections()
    queue = asyncio.Queue()
    # the consumer to be running in the background in infiinite loop, until I terminate it by adding a certain string to the queue
    consumer_tasks = [asyncio.create_task(consumer(queue, i)) for i in range(3)]
    producer_tasks = []

    i = 0
    items_produced = 0
    while i < 3:
        i += 1
        for j in range(2):
            record = f"{i}-record-{j}"
            print(f"record: {record}")
            items_produced += 1
            producer_task = asyncio.create_task(producer_task_copying(queue, record))
            producer_tasks.append(producer_task)
            # await producer_task_copying(queue, record)

    await asyncio.gather(*producer_tasks)
    print(f"\n\n\n\n\nBROKEN\n\n\n\n\n")
    length = queue.qsize()
    print(f"Queue length: {length}")
    for _ in range(3):
        await queue.put("STOP")
    # wait untile all remaining tasks are done
    await asyncio.gather(*consumer_tasks)
    # while queue.qsize() > 0:
    #     await asyncio.sleep(1)

    print("ALL FINISHED")
    print("items_produced", items_produced)
    print("items_consumed", items_consumed)


asyncio.run(main())
