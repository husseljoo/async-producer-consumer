import asyncio, asyncssh, sys, os
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
    sftp_con = await sftp_con.start_sftp_client()


async def producer_task_copying_connector(queue, file_name):
    global sftp_con
    await sftp_con.get(
        remotepaths=f"/root/sample_data/data1/{file_name}",
        localpath="/home/husseljo/damn/orange-files/python-stream",
    )

    print(f"STARTING to produce {file_name}\n")
    finished_record = f"{file_name}-finished"
    file_path = f"/home/husseljo/damn/orange-files/python-stream/{file_name}"
    await queue.put(file_path)
    print(f"producer_task_copying produced {finished_record}")


async def producer_task_copying(queue, record, time):
    print(f"STARTING to produce {record}\n")
    await asyncio.sleep(time)  # Simulate some asynchronous task
    finished_record = f"{record}-finished"
    await queue.put(finished_record)
    print(f"producer_task_copying produced {finished_record}")


async def consumer_connector(queue, id):
    global localmachine_con, items_consumed
    print(f"CONSUMER {id} STARTED")
    while True:
        file_path = await queue.get()
        print(f"item: {file_path}")
        if file_path == "STOP":
            # Terminate the consumer when "STOP" is encountered
            break
        file_name = os.path.basename(file_path)
        command = f"docker cp {file_path} namenode:/;docker exec namenode hadoop dfs -copyFromLocal -f {file_name} /python-async-data"
        result = await localmachine_con.run(command)
        print(f"Consumer {id}, exit_status for {file_name} output:", result.exit_status)
        if result.exit_status == 0 and os.path.exists(file_path):
            os.remove(file_path)
            print(f"File '{file_path}' removed successfully.")

        print(f"Consumer {id} copied {file_name} HDFS")
        items_consumed += 1
        queue.task_done()


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
    files = [
        "a.csv",
        "b.csv",
        "blud.csv",
        "gaga.csv",
        "husselj.csv",
        "oogaboog.csv",
        "sonman.csv",
        "wassup.csv",
        "yasta.csv",
        "zaz.csv",
    ]

    queue = asyncio.Queue()
    # the consumer to be running in the background in infiinite loop, until I terminate it by adding a certain string to the queue
    consumer_tasks = [
        asyncio.create_task(consumer_connector(queue, i)) for i in range(3)
    ]
    producer_tasks = []

    items_produced = 0
    for file in files:
        producer_task = asyncio.create_task(
            producer_task_copying_connector(queue, file)
        )
        producer_tasks.append(producer_task)

    print("\n\n\n\nGATHERING PRODUCERS.....\n\n\n\n")
    await asyncio.gather(*producer_tasks)
    print(f"\n\n\n\n\nBROKEN\n\n\n\n\n")
    length = queue.qsize()
    print(f"Queue length: {length}")
    for _ in range(3):
        await queue.put("STOP")
    # wait untile all remaining tasks are done
    print("\n\n\n\nGATHERING CONSUMERS.....\n\n\n\n")
    await asyncio.gather(*consumer_tasks)
    # while queue.qsize() > 0:
    #     await asyncio.sleep(1)

    print("ALL FINISHED")
    print("items_produced", items_produced)
    print("items_consumed", items_consumed)


asyncio.run(main())
