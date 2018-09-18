#      _____         __        __                               ____                                        __
#     / ___/ ____ _ / /____   / /_   __  __ _____ ____ _       / __ \ ___   _____ ___   ____ _ _____ _____ / /_
#     \__ \ / __ `// //_  /  / __ \ / / / // ___// __ `/      / /_/ // _ \ / ___// _ \ / __ `// ___// ___// __ \
#    ___/ // /_/ // /  / /_ / /_/ // /_/ // /   / /_/ /      / _, _//  __/(__  )/  __// /_/ // /   / /__ / / / /
#   /____/ \__,_//_/  /___//_.___/ \__,_//_/    \__, /      /_/ |_| \___//____/ \___/ \__,_//_/    \___//_/ /_/
#                                              /____/
#   Salzburg Research ForschungsgesmbH
#   Armin Niedermueller

#   OPC UA Server on Master Cluster
#   The purpose of this OPCUA client is to call the provided methods of the ConveyorBelt and Robot  and read
#   their (state) variables


from datetime import datetime
from opcua import Client
import pytz
import requests
import time
import sys

sys.path.insert(0, "..")


desired_distance = 0.55  # distance in meters to drive the belt
belt_velocity = 0.05428  # velocity of the belt in m/s (5.5cm/s)
timebuffer = 3           # time buffer for the wait loops after method call. wifi istn that fast
storage = []             # our storage data as an array

def move_belt(obj, direction, desired_distance):
    obj.call_method("2:MoveBelt", direction, desired_distance)  # drive 55cm right
    print("called move_belt to " + str(direction) + " for " + str(desired_distance) + "m")
    print("sleeping...")
    for i in range(0, (int(desired_distance / belt_velocity) * 10) + timebuffer):
        time.sleep(0.1)


def move_robot(obj, movement, place):
    obj.call_method("MoveRobot", movement, place)  # movement = PO,SO or PS # place = 1-9
    if movement == "PO":
        print("called move_robot from Printer to Output")
    elif movement == "SO":
        print("called move_robot from Storage #" + place + " to Output")
    elif movement == "PS":
        print("called move_robot from Printer to Storage #" + place)
    for i in range(0, (int(desired_distance / belt_velocity) * 10) + timebuffer):
        time.sleep(0.1)







if __name__ == "__main__":

    class SubHandler(object):

        """
        Subscription Handler. To receive events from server for a subscription
        data_change and event methods are called directly from receiving thread.
        Do not do expensive, slow or network operation there. Create another
        thread if you need to do such a thing
        """

        def datachange_notification(self, node, val, data):
            if data == "true":
                print("Python: New data change event", node, val, data)
                # METHOD CALLS
                # panda_finished = object_panda.call_method("MoveRobot", "SO", desired_storage)
                # while not panda_finished:
                #     time.sleep(0.3)
                #
                # pixtend_finished = object_pixtend.call_method("2:MoveBelt", "left", 0.6)
                # while not pixtend_finished:
                #     time.sleep(0.3)


        def event_notification(self, event):
            print("Python: New event", event)


    url_opcua_adapter = "192.168.48.81:1337"
    client_panda = Client("opc.tcp://192.168.48.41:4840/freeopcua/server/")
    client_pixtend = Client("opc.tcp://192.168.48.42:4840/freeopcua/server/")
    #client_fhs = Client("opc.tcp://192.168.10.138:4840")
    client_fhs = Client("opc.tcp://192.168.48.44:4840/freeopcua/server/")
    # client = Client("opc.tcp://admin@localhost:4840/freeopcua/server/") #connect using a user

    # get storage data from file
    # [1][2][3]
    # [4][5][6]
    # [7][8][9]
    with open("./dtz_storage", "r", encoding="utf-8") as inputfile:
        for line in inputfile:
            storage.append(line)


    try:
        client_panda.connect()
        client_pixtend.connect()
        client_fhs.connect()

        # Client has a few methods to get proxy to UA nodes that should always be in address space such as Root or Objects
        root_panda = client_panda.get_root_node()
        root_pixtend = client_pixtend.get_root_node()
        root_fhs = client_fhs.get_root_node()

        # get a specific node knowing its node id
        # var = client.get_node(ua.NodeId(1002, 2))
        # var = client.get_node("ns=3;i=2002")
        # print(var)
        # var.get_data_value() # get value of node as a DataValue object
        # var.get_value() # get value of node as a python builtin
        # var.set_value(ua.Variant([23], ua.VariantType.Int64)) #set node value using explicit data type
        # var.set_value(3.9) # set node value using implicit data type


        # Now getting a variable node using its browse path


        # object_fhs = root_fhs.get_child("0:Objects", "4:PLC", "6:Modules", "6:::","6:Global PV","6:FunctionalPlcKuka")
        object_fhs = root_fhs.get_child(["0:Objects", "2:PLC"])
        object_panda = root_panda.get_child(["0:Objects", "2:PandaRobot"])
        object_pixtend = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt"])

        mover_panda = root_panda.get_child(["0:Objects", "2:PandaRobot", "2:MoveRobot"])
        mover_pixtend = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt", "2:MoveBelt"])

        # desired_storage = object_fhs.get_child("6:ShelfNumber")
        # new_val_available = root_fhs.get_child("6:NewValueAvailable")
        # task_running = root_fhs.get_child("6:TaskRunning")

        desired_shelf = object_fhs.get_child(["2:ShelfNumber"])
        new_val_available = object_fhs.get_child(["2:NewValueAvailable"])
        task_running = object_fhs.get_child(["2:TaskRunning"])

        #handler = SubHandler()
        #sub = client_fhs.create_subscription(500, handler)
        #handle = sub.subscribe_data_change(new_val_available)
        #time.sleep(0.1)

        #while True:
        print("running")
        # VALUES
        panda_state = root_panda.get_child(["0:Objects", "2:PandaRobot", "2:RobotState"])
        # panda_temp_value = root_panda.get_child(["0:Objects", "2:Object1", "2:RobotTempValue"])
        conbelt_state = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt", "2:ConBeltState"])
        conbelt_dist = root_pixtend.get_child(["0:Objects", "2:ConveyorBelt", "2:ConBeltDist"])


        tm = datetime.utcnow().replace(tzinfo=pytz.UTC).isoformat()
        #r1 = requests.post(url_opcua_adapter, data={'id': 'pandapc.panda_state', 'timestamp': tm, 'panda_state': panda_state})
        #r2 = requests.post(url_opcua_adapter, data={'id': 'pixtend.conbelt_state', 'timestamp': tm, 'conbelt_state': conbelt_state})
        #r3 = requests.post(url_opcua_adapter, data={'id': 'pandapc.conbelt_dist', 'timestamp': tm, 'conbelt_dist': conbelt_dist})

        time.sleep(0.3)


        # STORAGE OUTPUT

        print("---------------------------")
        local_shelf = desired_shelf.get_value()
        print("Desired shelf is " + str(local_shelf))

        print("---------------------------")
        print("Storage containing " + str(len(storage)) + " fields")
        i = 0
        while i < len(storage):
            print("[" + str(i+1) + "]: " + str(storage[i]), end="")
            i = i + 1

        print("\n---------------------------")


        # STORAGE CHECK

        if (storage[local_shelf-1] == 1):
            print("Desired shelf is not empty")

        else:
            print("Desired shelf is empty")

        print(local_shelf)
        print(storage[local_shelf-1])




    except KeyboardInterrupt:
        print("\nClient stopped...CTRL+C pressed")
    except requests.exceptions.ConnectionError:
        print("error connecting...")
    finally:
        print("\nClient stopped")
        client_pixtend.disconnect()
        client_panda.disconnect()

        # write storage data to file
        # [1][2][3]
        # [4][5][6]
        # [7][8][9]
        print("lel")

        with open("./dtz_storage", "w", encoding="utf-8") as outputfile:
            for i in storage:
                outputfile.write(str(i))
